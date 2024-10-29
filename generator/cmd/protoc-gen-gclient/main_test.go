package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestRun_Rust(t *testing.T) {
	tDir := t.TempDir()
	if err := run("testdata/rust/rust.bin", tDir, "../../templates"); err != nil {
		t.Fatal(err)
	}
	diff(t, "testdata/rust/golden", tDir)
}

func diff(t *testing.T, goldenDir, outputDir string) {
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatal(err)
	}
	if *updateGolden {
		for _, f := range files {
			b, err := os.ReadFile(filepath.Join(outputDir, f.Name()))
			if err != nil {
				t.Fatal(err)
			}
			outFileName := filepath.Join(goldenDir, f.Name())
			t.Logf("writing golden file %s", outFileName)
			if err := os.WriteFile(outFileName, b, os.ModePerm); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	for _, f := range files {
		want, err := os.ReadFile(filepath.Join(goldenDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(outputDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("mismatch(-want, +got): %s", diff)
		}
	}
}
