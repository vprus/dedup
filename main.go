
package main

import (
	"fmt"
	"os"
  "log"
  "io"
  "encoding/hex"
  "path/filepath"
  "github.com/urfave/cli/v2"
  "github.com/minio/highwayhash"
)

func checksumOfFile(path string) []byte {
    key, err := hex.DecodeString("08ed80781e731f756ad4deefa19d3691787654241f0cda6013b2b82c7c391555")
    if err != nil {
      fmt.Printf("Cannot decode hex key: %v", err) // add error handling
      return nil
    }
  
    file, err := os.Open(path)
    if err != nil {
      fmt.Printf("Failed to open the file: %v", err) // add error handling
      return nil
    }
    defer file.Close()
  
    hash, err := highwayhash.New(key)
    if err != nil {
      fmt.Printf("Failed to create HighwayHash instance: %v", err) // add error handling
      return nil
    }
  
    if _, err = io.Copy(hash, file); err != nil {
      fmt.Printf("Failed to read from file: %v", err) // add error handling
      return nil
    }
  
    checksum := hash.Sum(nil)
    //return hex.EncodeToString(checksum)
    return checksum
}

func checksums(root string) map[string][]byte {
  r := make(map[string][]byte)
  err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
    if !info.IsDir() {
      c := checksumOfFile(path)
      r[path] = c
    }
    return nil
  })
  if (err != nil) {
    panic(err)
  }
  return r
}

func main() {
	app := &cli.App{
		Name: "archiver",
    Usage: "lightweight tool to archive data",
    Commands: []*cli.Command{
      {
        Name:    "checksum",
        Usage:   "Compute checksums for all files in a directory",
        Action:  func(c *cli.Context) error {
          if c.Args().Len() != 2 {
            return fmt.Errorf("incorrect number of arguments for 'checksum' command")
          }


          path := c.Args().First()
          output := c.Args().Get(1)

          r := checksums(path)
          fmt.Printf("Found %d files\n", len(r))
          writeParquet(output, r)
          for k, v := range r {
            fmt.Printf("%s: %s\n", k, hex.EncodeToString(v))
          }
          return nil
        },
      },
    },
  }
	
	  err := app.Run(os.Args)
	  if err != nil {
      log.Fatal(err)
      fmt.Println(app.Usage)
	  }
}