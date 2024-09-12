package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Answer struct {
	Text string `json:"text"`
}

type QA struct {
	Question     string   `json:"question"`
	IsImpossible bool     `json:"is_impossible,omitempty"`
	Answers      []Answer `json:"answers"`
}

type Paragraph struct {
	QAs []QA `json:"qas"`
}

type Article struct {
	Paragraphs []Paragraph `json:"paragraphs"`
}

type SQuAD struct {
	Data []Article `json:"data"`
}
type RequestBody struct {
	Model            string    `json:"model"`
	Messages         []Message `json:"messages"`
	TopP             float64   `json:"top_p"`
	N                int       `json:"n"`
	MaxTokens        int       `json:"max_tokens"`
	Stream           bool      `json:"stream"`
	FrequencyPenalty float64   `json:"frequency_penalty"`
	Stop             []string  `json:"stop"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func main() {
	// define command-line arguments
	//url := "http://kate-nim-meta-llama3-8b-instruct-gw.default-tenant.app.llm-dev.iguazio-cd1.com/v1/chat/completions"
	url := "http://kate-nim-proxy.default-tenant.app.llm-dev.iguazio-cd1.com/v1/chat/completions"
	totalRequests := 100
	maxParallel := 10

	questions, answers := readDS()

	// waitGroup to synchronize completion of goroutines
	var wg sync.WaitGroup

	// channel to receive timing results from goroutines
	results := make(chan time.Duration, totalRequests)

	// semaphore to limit the number of concurrent goroutines
	sem := make(chan struct{}, maxParallel)

	// iterate over the total number of requests and spawn goroutines to send requests concurrently
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		sem <- struct{}{} // acquire semaphore slot

		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }() // release semaphore slot

			// Create the request body with a new question
			reqBody := RequestBody{
				Model: "meta/llama3-8b-instruct",
				Messages: []Message{
					{
						Role:    "user",
						Content: questions[i%len(questions)+8000], // cycle through the questions
					},
				},
				TopP:             1,
				N:                1,
				MaxTokens:        128,
				Stream:           true,
				FrequencyPenalty: 1.0,
				Stop:             []string{"hello"},
			}

			// Convert the request body to JSON
			jsonData, err := json.Marshal(reqBody)
			if err != nil {
				fmt.Printf("Error marshaling JSON: %v\n", err)
				return
			}

			start := time.Now()
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				fmt.Printf("Error sending request: %v\n", err)
				return
			}
			defer resp.Body.Close()

			elapsed := time.Since(start)
			results <- elapsed

			// Read the response body
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("Error reading response: %v\n", err)
				return
			}

			// Parse the streamed response
			realAnswer := parseStreamData(string(body))

			// Print the question, expected answer, and real answer
			fmt.Printf("Question: %s\n", questions[i%len(questions)])
			fmt.Printf("Expected Answer: %s\n", answers[i%len(answers)])
			fmt.Printf("Actual Answer: %s\n", realAnswer)
			fmt.Printf("Response Time: %v\n", elapsed)
			fmt.Println("------------------------------")
		}(i)
	}

	// wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// collect timing results from the channel and calculate statistics
	totalTime := time.Duration(0)
	numCompleted := 0

	for elapsed := range results {
		totalTime += elapsed
		numCompleted++
		//fmt.Printf("Request took: %v\n", elapsed)
	}

	// calculate average response time
	if numCompleted > 0 {
		avgTime := totalTime / time.Duration(numCompleted)
		fmt.Printf("Average response time: %v\n", avgTime)
		fmt.Printf("%d of %d requests succeeded\n", numCompleted, totalRequests)
	} else {
		fmt.Println("No requests were successful")
	}
}

func readDS() ([]string, []string) {
	// Open and read the JSON file
	file, err := os.Open("/Users/Ekaterina_Molchanova/iguazio/misc_workspace/nuclio/hack/scripts/benchmark/nim/SQuAD.json")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, nil
	}
	defer file.Close()

	byteValue, _ := ioutil.ReadAll(file)

	// Unmarshal the JSON data
	var squadData SQuAD
	json.Unmarshal(byteValue, &squadData)

	// Initialize slices to store questions and answers
	var questions []string
	var answers []string

	// Iterate through the data to extract questions and answers
	for _, article := range squadData.Data {
		for _, paragraph := range article.Paragraphs {
			for _, qa := range paragraph.QAs {
				questions = append(questions, qa.Question)
				if qa.IsImpossible {
					answers = append(answers, "")
				} else if len(qa.Answers) > 0 {
					answers = append(answers, qa.Answers[0].Text)
				} else {
					answers = append(answers, "")
				}
			}
		}
	}
	return questions, answers
}

// parseStreamData simulates the parsing of the streamed response
func parseStreamData(streamData string) string {
	// Split by double newlines to separate each "data:" chunk
	chunks := strings.Split(streamData, "\n\n")

	// Iterate over each chunk, strip "data: " prefix and parse as JSON
	var parsedData strings.Builder
	for _, chunk := range chunks {
		if strings.HasPrefix(chunk, "data: ") {
			jsonData := strings.TrimPrefix(chunk, "data: ")
			var parsed map[string]interface{}
			err := json.Unmarshal([]byte(jsonData), &parsed)
			if err != nil {
				continue
			}

			// Extract the content from the parsed data
			if choices, ok := parsed["choices"].([]interface{}); ok {
				if delta, ok := choices[0].(map[string]interface{})["delta"].(map[string]interface{}); ok {
					if content, ok := delta["content"].(string); ok {
						parsedData.WriteString(content)
					}
				}
			}
		}
	}
	return parsedData.String()
}
