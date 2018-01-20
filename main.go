package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func panicIfErr(err error) {
	if(err != nil){
		panic(err)
	}
}
func handler() {
	tmpFilePath := "/tmp/complaints.csv"
	out, err := os.Create(tmpFilePath)
	panicIfErr(err)
	defer out.Close()

	fmt.Printf("Downloading complaints, storing in %s\n", tmpFilePath)

	resp, _ := http.Get("https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv?accessType=DOWNLOAD")
	panicIfErr(err)
	defer resp.Body.Close()

	size, err := io.Copy(out, resp.Body)
	panicIfErr(err)

	targetMinSizeMB := int64(250)
	oneMB := int64(1024*1024)
	if size/oneMB < targetMinSizeMB {
		fmt.Printf("File not large enough (only %v bytes); something went wrong. Quitting...\n", size)
	} else {
		fmt.Printf("Pushing file of size %v MB to S3...", size/oneMB)
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("us-east-1")},
		)
		panicIfErr(err)
	
		uploader := s3manager.NewUploader(sess)
		bucket := "scrubbedconsumercomplaints"
		filename := "complaints.csv"
		file, err := os.Open(tmpFilePath)

		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key: aws.String(filename),
			Body: file,
		})
		panicIfErr(err)
		
		fmt.Printf("Successfully uploaded %q to %q\n", filename, bucket)
	}
}

func main() {
	//for now, just mimic the logic inside of lambda.Start() to determine whether it's running in AWS
	if os.Getenv("_LAMBDA_SERVER_PORT") != "" {
		fmt.Println("Running in Lambda...")
		lambda.Start(handler)
	} else {
		fmt.Println("Running locally...")
		handler() 
	}
}
