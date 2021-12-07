use std::path::Path;
use std::path::PathBuf;

use bytes::Buf;
use bytes::BytesMut;
use reqwest::Response;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use super::{Error, Result};
pub use bytes::Bytes;

#[derive(Debug)]
pub struct BytesReader {
    inner: Inner,
}

impl BytesReader {
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        BytesReader {
            inner: Inner::File(FileReader {
                path: path.as_ref().to_path_buf(),
                file: None,
            }),
        }
    }

    pub async fn read_all(self) -> Result<Bytes> {
        let bytes = match self.inner {
            Inner::File(file_reader) => {
                let mut file = File::open(&file_reader.path).await.map_err(Error::wrap)?;
                let len = file.metadata().await.map_err(Error::wrap)?.len();
                let mut buf = BytesMut::with_capacity(len as usize);
                file.read_buf(&mut buf).await.map_err(Error::wrap)?;
                buf.freeze()
            }
            Inner::Byte(byte_reader) => byte_reader.bytes,
            Inner::Response(response_reader) => response_reader
                .into_inner()
                .bytes()
                .await
                .map_err(Error::wrap)?,
        };
        Ok(bytes)
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.inner {
            Inner::File(file_reader) => file_reader.read(buf).await,
            Inner::Byte(bytes_reader) => {
                std::io::Read::read(bytes_reader, buf).map_err(Error::wrap)
            }
            Inner::Response(response_reader) => response_reader.read(buf).await,
        }
    }
}

impl From<Bytes> for BytesReader {
    fn from(bytes: bytes::Bytes) -> Self {
        BytesReader {
            inner: Inner::Byte(ByteReader { offset: 0, bytes }),
        }
    }
}

impl From<Vec<u8>> for BytesReader {
    fn from(bytes: Vec<u8>) -> Self {
        BytesReader {
            inner: Inner::Byte(ByteReader {
                bytes: Bytes::from(bytes),
                offset: 0,
            }),
        }
    }
}

impl From<&'static [u8]> for BytesReader {
    fn from(slice: &'static [u8]) -> Self {
        BytesReader {
            inner: Inner::Byte(ByteReader {
                bytes: Bytes::from_static(slice),
                offset: 0,
            }),
        }
    }
}

impl From<&'static str> for BytesReader {
    fn from(slice: &'static str) -> Self {
        BytesReader {
            inner: Inner::Byte(ByteReader {
                bytes: Bytes::from_static(slice.as_bytes()),
                offset: 0,
            }),
        }
    }
}

impl From<String> for BytesReader {
    fn from(s: String) -> Self {
        BytesReader {
            inner: Inner::Byte(ByteReader {
                bytes: Bytes::from(s.into_bytes()),
                offset: 0,
            }),
        }
    }
}

impl From<Response> for BytesReader {
    fn from(response: Response) -> Self {
        Self {
            inner: Inner::Response(ResponseReader {
                response,
                extra: None,
            }),
        }
    }
}

#[derive(Debug)]
enum Inner {
    File(FileReader),
    Byte(ByteReader),
    Response(ResponseReader),
}

#[derive(Debug)]
struct FileReader {
    path: PathBuf,
    file: Option<File>,
}

impl FileReader {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.file.is_none() {
            let file_handle = File::open(&self.path).await.map_err(Error::wrap)?;
            self.file = Some(file_handle);
        }
        let n = self
            .file
            .as_mut()
            .unwrap()
            .read(buf)
            .await
            .map_err(Error::wrap)?;
        Ok(n)
    }
}

#[derive(Debug)]
struct ByteReader {
    bytes: Bytes,
    offset: usize,
}

impl std::io::Read for ByteReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.bytes.slice(self.offset..).reader().read(buf) {
            Ok(bytes_read) => {
                self.offset += bytes_read;
                Ok(bytes_read)
            }
            Err(err) => Err(err),
        }
    }
}

#[derive(Debug)]
struct ResponseReader {
    response: reqwest::Response,
    extra: Option<Bytes>,
}

impl ResponseReader {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut bytes_read_to_buf: usize = 0;

        if let Some(bytes) = &mut self.extra {
            if bytes.len() <= buf.len() {
                let n = std::io::Read::read(&mut bytes.reader(), buf).map_err(Error::wrap)?;
                bytes_read_to_buf = n;
                self.extra = None;
            } else {
                let cur_chunk = bytes.split_to(buf.len());
                let n = std::io::Read::read(&mut cur_chunk.reader(), buf).map_err(Error::wrap)?;
                return Ok(n);
            }
        }

        while let Some(mut chunk) = self.response.chunk().await.map_err(Error::wrap)? {
            if (chunk.len() + bytes_read_to_buf) <= buf.len() {
                let n = std::io::Read::read(&mut chunk.reader(), &mut buf[bytes_read_to_buf..])
                    .map_err(Error::wrap)?;
                bytes_read_to_buf = n;
            } else {
                let cur_chunk = chunk.split_to(buf.len());
                self.extra = Some(chunk);
                let n = std::io::Read::read(&mut cur_chunk.reader(), &mut buf[bytes_read_to_buf..])
                    .map_err(Error::wrap)?;
                bytes_read_to_buf = n;
                break;
            }
        }
        Ok(bytes_read_to_buf)
    }

    fn into_inner(self) -> reqwest::Response {
        self.response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::main]
    #[test]
    async fn byte_reader_read() {
        let my_vec = "this is a test".as_bytes().to_vec();
        let mut br = BytesReader::from(my_vec);
        let mut buf = vec![0; 7];
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!("this is".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!(" a test".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(0, n);
    }

    #[tokio::main]
    #[test]
    async fn byte_reader_read_all() {
        let my_vec = "this is a test".as_bytes().to_vec();
        let br = BytesReader::from(my_vec);
        let bytes = br.read_all().await.unwrap();
        assert_eq!("this is a test".as_bytes(), bytes);
    }

    #[tokio::main]
    #[test]
    async fn file_reader_read() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/test/file.txt");
        let mut br = BytesReader::from_path(path);
        let mut buf = vec![0; 7];
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!("this is".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!(" a test".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!(" from a".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(6, n);
        // There is garbage in the buffer from a previous read. Only check n u8s.
        assert_eq!(" file.".as_bytes(), &buf[..n]);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(0, n);
    }

    #[tokio::main]
    #[test]
    async fn file_reader_read_all() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/test/file.txt");
        let br = BytesReader::from_path(path);
        let bytes = br.read_all().await.unwrap();
        assert_eq!("this is a test from a file.".as_bytes(), &bytes);
    }

    #[tokio::main]
    #[test]
    async fn response_reader_read() {
        let http_response = http::Response::builder()
            .body("this is a mock network resp")
            .unwrap();
        let response = Response::from(http_response);
        let mut br = BytesReader::from(response);
        let mut buf = vec![0; 7];
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!("this is".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!(" a mock".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(7, n);
        assert_eq!(" networ".as_bytes(), &buf);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(6, n);
        assert_eq!("k resp".as_bytes(), &buf[..n]);
        let n = br.read(&mut buf).await.unwrap();
        assert_eq!(0, n);
    }

    #[tokio::main]
    #[test]
    async fn response_reader_read_all() {
        let http_response = http::Response::builder()
            .body("this is a mock network response")
            .unwrap();
        let response = Response::from(http_response);
        let br = BytesReader::from(response);
        let bytes = br.read_all().await.unwrap();
        assert_eq!("this is a mock network response".as_bytes(), &bytes);
    }
}
