package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Client struct {
	client  *s3.Client
	bucket  string
	cfg     *Config
	metrics *MetricsCollector
	verbose bool
}

func NewS3Client(cfg *Config, metrics *MetricsCollector) (*S3Client, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.Region))

	// Build TLS config for custom CA or skip-verify
	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("TLS setup failed: %w", err)
	}

	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.DialContext = (&net.Dialer{
			Timeout: time.Duration(cfg.ConnectTimeoutMs) * time.Millisecond,
		}).DialContext
		t.ResponseHeaderTimeout = time.Duration(cfg.RequestTimeoutMs) * time.Millisecond
		t.MaxIdleConns = 500
		t.MaxIdleConnsPerHost = 500
		if tlsConfig != nil {
			t.TLSClientConfig = tlsConfig
		}
	})
	opts = append(opts, config.WithHTTPClient(httpClient))

	// ClickHouse retry configuration:
	//   s3_retry_attempts = 10 (inner SDK retries, exponential backoff)
	//   Backoff: 0→50→100→200→400→800→1600→3200→6400ms
	opts = append(opts, config.WithRetryMaxAttempts(cfg.MaxRetryAttempts+1))

	if cfg.UseAWSChain || cfg.Profile != "" {
		if cfg.Profile != "" {
			opts = append(opts, config.WithSharedConfigProfile(cfg.Profile))
		}
	} else if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		))
	}

	if cfg.Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.Endpoint,
					HostnameImmutable: cfg.PathStyle,
				}, nil
			},
		)
		opts = append(opts, config.WithEndpointResolverWithOptions(resolver))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.PathStyle
	})

	return &S3Client{
		client:  client,
		bucket:  cfg.Bucket,
		cfg:     cfg,
		metrics: metrics,
		verbose: cfg.Verbose,
	}, nil
}

func (s *S3Client) EnsureBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(s.bucket),
		})
		if err != nil {
			return fmt.Errorf("cannot create bucket %s: %w", s.bucket, err)
		}
	}
	return nil
}

// PutObject uploads a single object, choosing single PUT or multipart based on
// s3_max_single_part_upload_size (32 MB default).
func (s *S3Client) PutObject(ctx context.Context, key string, size int64) error {
	s.metrics.TrackConcurrency(1)
	defer s.metrics.TrackConcurrency(-1)

	if size > s.cfg.MaxSinglePartUploadSize {
		return s.putMultipart(ctx, key, size)
	}
	return s.putSingle(ctx, key, size)
}

func (s *S3Client) putSingle(ctx context.Context, key string, size int64) error {
	data := makeRandomData(size)
	opType := OpPutSmall
	if size > 1024*1024 {
		opType = OpPutLarge
	}

	start := time.Now()
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(size),
	})
	elapsed := time.Since(start)
	s.metrics.RecordOp(opType, elapsed, size, err)

	// Track timeout violations (ClickHouse: > 30s = retry storm trigger)
	if elapsed.Milliseconds() > int64(s.cfg.RequestTimeoutMs) {
		s.metrics.TimeoutViolations.Add(1)
	}

	return err
}

func (s *S3Client) putMultipart(ctx context.Context, key string, totalSize int64) error {
	s.metrics.MultipartInitiated.Add(1)

	start := time.Now()
	createOut, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	s.metrics.RecordOp(OpCreateMultipart, time.Since(start), 0, err)
	if err != nil {
		s.metrics.MultipartFailed.Add(1)
		return fmt.Errorf("CreateMultipartUpload: %w", err)
	}
	uploadID := *createOut.UploadId

	// ClickHouse variable part sizes:
	//   s3_min_upload_part_size = 16 MB
	//   s3_upload_part_size_multiply_factor = 2
	//   s3_upload_part_size_multiply_parts_count_threshold = 500
	//   s3_max_inflight_parts_for_one_file = 20
	var completedParts []types.CompletedPart
	var offset int64
	partNum := int32(1)
	partSize := s.cfg.MinUploadPartSize

	for offset < totalSize {
		remaining := totalSize - offset
		currentPartSize := partSize
		if currentPartSize > remaining {
			currentPartSize = remaining
		}

		data := makeRandomData(currentPartSize)
		partStart := time.Now()
		uploadOut, uploadErr := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(s.bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(partNum),
			Body:       bytes.NewReader(data),
		})
		s.metrics.RecordOp(OpUploadPart, time.Since(partStart), currentPartSize, uploadErr)

		if uploadErr != nil {
			s.abortMultipart(ctx, key, uploadID)
			s.metrics.MultipartFailed.Add(1)
			return fmt.Errorf("UploadPart %d: %w", partNum, uploadErr)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadOut.ETag,
			PartNumber: aws.Int32(partNum),
		})

		offset += currentPartSize
		partNum++
		// CH doubles part size every s3_upload_part_size_multiply_parts_count_threshold (500) parts
		if int(partNum)%s.cfg.UploadPartSizeMultThresh == 0 {
			partSize *= int64(s.cfg.UploadPartSizeMultFactor)
		}
	}

	completeStart := time.Now()
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	s.metrics.RecordOp(OpCompleteMultipart, time.Since(completeStart), 0, err)

	if err != nil {
		s.metrics.MultipartFailed.Add(1)
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	s.metrics.MultipartCompleted.Add(1)
	return nil
}

func (s *S3Client) abortMultipart(ctx context.Context, key, uploadID string) {
	start := time.Now()
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	s.metrics.RecordOp(OpAbortMultipart, time.Since(start), 0, err)
	s.metrics.MultipartAborted.Add(1)
}

// GetObject performs a full GET (used for primary.idx, marks, metadata).
func (s *S3Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	s.metrics.TrackConcurrency(1)
	defer s.metrics.TrackConcurrency(-1)

	start := time.Now()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		s.metrics.RecordOp(OpGetFull, time.Since(start), 0, err)
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	elapsed := time.Since(start)
	s.metrics.RecordOp(OpGetFull, elapsed, int64(len(data)), err)

	if elapsed.Milliseconds() > int64(s.cfg.RequestTimeoutMs) {
		s.metrics.TimeoutViolations.Add(1)
	}

	return data, err
}

// GetObjectRange performs a range GET (bytes=offset-offset+length-1).
// CH uses 64 KB granules for column data reads.
func (s *S3Client) GetObjectRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.metrics.TrackConcurrency(1)
	defer s.metrics.TrackConcurrency(-1)

	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	start := time.Now()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		s.metrics.RecordOp(OpGetRange, time.Since(start), 0, err)
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	elapsed := time.Since(start)
	s.metrics.RecordOp(OpGetRange, elapsed, int64(len(data)), err)

	if elapsed.Milliseconds() > int64(s.cfg.RequestTimeoutMs) {
		s.metrics.TimeoutViolations.Add(1)
	}

	return data, err
}

// HeadObject checks if an object exists and returns its size.
func (s *S3Client) HeadObject(ctx context.Context, key string) (int64, error) {
	s.metrics.TrackConcurrency(1)
	defer s.metrics.TrackConcurrency(-1)

	start := time.Now()
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	var size int64
	if err == nil && out.ContentLength != nil {
		size = *out.ContentLength
	}
	s.metrics.RecordOp(OpHeadObject, time.Since(start), 0, err)
	return size, err
}

// DeleteObjects performs a batch delete (up to 1000 keys per request).
// CH uses this for merge cleanup — MUST be supported.
func (s *S3Client) DeleteObjects(ctx context.Context, keys []string) error {
	s.metrics.TrackConcurrency(1)
	defer s.metrics.TrackConcurrency(-1)

	objects := make([]types.ObjectIdentifier, len(keys))
	for i, k := range keys {
		objects[i] = types.ObjectIdentifier{Key: aws.String(k)}
	}

	start := time.Now()
	out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	})
	s.metrics.RecordOp(OpDeleteObjects, time.Since(start), 0, err)

	if err != nil {
		return err
	}
	if len(out.Errors) > 0 {
		return fmt.Errorf("DeleteObjects partial failure: %d errors, first: %s",
			len(out.Errors), aws.ToString(out.Errors[0].Message))
	}
	return nil
}

// ListObjects lists objects under a prefix with pagination.
func (s *S3Client) ListObjects(ctx context.Context, prefix string, maxKeys int32) ([]string, error) {
	s.metrics.TrackConcurrency(1)
	defer s.metrics.TrackConcurrency(-1)

	var allKeys []string
	var contToken *string

	for {
		start := time.Now()
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int32(maxKeys),
			ContinuationToken: contToken,
		})
		s.metrics.RecordOp(OpListObjects, time.Since(start), 0, err)
		if err != nil {
			return allKeys, err
		}

		for _, obj := range out.Contents {
			allKeys = append(allKeys, aws.ToString(obj.Key))
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		contToken = out.NextContinuationToken
	}
	return allKeys, nil
}

// CreateMultipartUploadRaw exposes the raw API for failure testing.
func (s *S3Client) CreateMultipartUploadRaw(ctx context.Context, key string) (string, error) {
	start := time.Now()
	out, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	s.metrics.RecordOp(OpCreateMultipart, time.Since(start), 0, err)
	if err != nil {
		return "", err
	}
	s.metrics.MultipartInitiated.Add(1)
	return aws.ToString(out.UploadId), nil
}

// UploadPartRaw uploads a single part in a multipart upload.
func (s *S3Client) UploadPartRaw(ctx context.Context, key, uploadID string, partNum int32, data []byte) (string, error) {
	start := time.Now()
	out, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNum),
		Body:       bytes.NewReader(data),
	})
	s.metrics.RecordOp(OpUploadPart, time.Since(start), int64(len(data)), err)
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// CompleteMultipartUploadRaw completes a multipart upload.
func (s *S3Client) CompleteMultipartUploadRaw(ctx context.Context, key, uploadID string, parts []types.CompletedPart) error {
	start := time.Now()
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	s.metrics.RecordOp(OpCompleteMultipart, time.Since(start), 0, err)
	if err == nil {
		s.metrics.MultipartCompleted.Add(1)
	} else {
		s.metrics.MultipartFailed.Add(1)
	}
	return err
}

// AbortMultipartUploadRaw aborts a multipart upload.
func (s *S3Client) AbortMultipartUploadRaw(ctx context.Context, key, uploadID string) error {
	start := time.Now()
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	s.metrics.RecordOp(OpAbortMultipart, time.Since(start), 0, err)
	s.metrics.MultipartAborted.Add(1)
	return err
}

// ListMultipartUploads lists in-progress multipart uploads.
func (s *S3Client) ListMultipartUploads(ctx context.Context) (int, error) {
	start := time.Now()
	out, err := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.bucket),
	})
	s.metrics.RecordOp(OpListMultipart, time.Since(start), 0, err)
	if err != nil {
		return 0, err
	}
	return len(out.Uploads), nil
}

// CleanupPrefix deletes all objects under a prefix.
func (s *S3Client) CleanupPrefix(ctx context.Context, prefix string) error {
	keys, err := s.ListObjects(ctx, prefix, 1000)
	if err != nil {
		return err
	}
	for len(keys) > 0 {
		batch := keys
		if len(batch) > 1000 {
			batch = batch[:1000]
		}
		if err := s.DeleteObjects(ctx, batch); err != nil {
			return err
		}
		keys = keys[len(batch):]
	}
	return nil
}

func buildTLSConfig(cfg *Config) (*tls.Config, error) {
	if cfg.TLSCACert == "" && !cfg.TLSSkipVerify {
		if cfg.Endpoint != "" && strings.HasPrefix(cfg.Endpoint, "https://") {
			return &tls.Config{MinVersion: tls.VersionTLS12}, nil
		}
		return nil, nil
	}

	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLSSkipVerify,
	}

	if cfg.TLSCACert != "" {
		caCert, err := os.ReadFile(cfg.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("reading CA cert %s: %w", cfg.TLSCACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", cfg.TLSCACert)
		}
		tc.RootCAs = pool
	}

	return tc, nil
}

func makeRandomData(size int64) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}
