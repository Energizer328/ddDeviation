package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"
)

type dataPoint struct {
	query string
	group string
	time  *time.Time
	value float64
}

func parseCSV(path string) ([]*dataPoint, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer f.Close()

	res := []*dataPoint{}
	reader := csv.NewReader(f)

	i := -1
	for {
		i++

		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to get the record at position %d: %v", i, err)
		}

		if i == 0 {
			continue
		}

		if len(record) != 4 {
			return nil, fmt.Errorf("unexpected record length at position %d", i)
		}

		query := record[0]
		group := record[1]

		t, err := time.Parse(time.RFC3339Nano, record[2])
		if err != nil {
			return nil, fmt.Errorf("failed to parse time for record at position %d: %v", i, err)
		}

		value, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse value or record at position %d, %v", i, err)
		}

		dp := dataPoint{
			query: query,
			group: group,
			time:  &t,
			value: value,
		}

		res = append(res, &dp)
	}

	return res, nil
}

func getAverage(d []float64) float64 {
	sum := 0.0
	for _, v := range d {
		sum += v
	}
	return sum / float64(len(d))
}

func getStdDeviation(d []float64) (float64, float64) {
	avg := getAverage(d)
	sqrDeviations := make([]float64, len(d))

	for i, v := range d {
		sqrDeviations[i] = math.Pow((v - avg), 2)
	}

	sigma2 := getAverage(sqrDeviations)

	return avg, math.Sqrt(sigma2)
}

func main() {
	args := os.Args
	if args[1] == "" {
		fmt.Print("missing file argument\n")
		os.Exit(1)
	}

	data, err := parseCSV(args[1])
	if err != nil {
		fmt.Printf("failed to parse CSV: %v", err)
		os.Exit(1)
	}

	values := make([]float64, len(data))
	for i, d := range data {
		values[i] = d.value
	}

	avg, deviation := getStdDeviation(values)
	fmt.Printf("Avg: %f\n", avg)
	fmt.Printf("Standard deviation: %f\n", deviation)
}
