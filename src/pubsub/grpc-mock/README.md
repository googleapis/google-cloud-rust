# A mockable Pub/Sub service implementation

A fake gRPC server for the Pub/Sub service. This is specifically helpful for
testing the handwritten code for the bidirectional `StreamingPull` RPC.

This is analogous to the [mockable GCS+gRPC service][gcs], which you should read
for more details on the design. Apologies for the redirect, but I would rather
not maintain separate documents.

[gcs]: /src/storage/grpc-mock/README.md
