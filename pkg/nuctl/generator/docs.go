package main

import (
	"fmt"
	"github.com/nuclio/nuclio/pkg/nuctl/command"
	"github.com/spf13/cobra/doc"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: generate_docs <output_path>")
		os.Exit(1)
	}

	outputPath := os.Args[1]
	cmd := command.NewRootCommandeer().GetCmd()
	cmd.DisableAutoGenTag = true

	// Assuming rootCmd is defined in your main.go or similar file
	if err := doc.GenMarkdownTree(cmd, outputPath); err != nil {
		log.Fatalf("Failed to generate Markdown documentation: %v", err)
	}

	fmt.Printf("Documentation generated at: %s\n", outputPath)
}
