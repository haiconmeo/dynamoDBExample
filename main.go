package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Book struct {
	Id     int    `json:"id"`
	Name   string `json:"name"`
	Author string `json:"author"`
}

type BookRepository interface {
	Create(book *Book) error
	GetById(id int) (*Book, error)
	Update(book *Book) error
	Delete(id int) error
	List() ([]*Book, error)
}
type BookUseCase struct {
	repo BookRepository
}

func NewBookUseCase(repo BookRepository) *BookUseCase {
	return &BookUseCase{repo: repo}
}

func (uc *BookUseCase) createBook(book *Book) error {
	return uc.repo.Create(book)
}

func (uc *BookUseCase) GetById(id int) (*Book, error) {
	return uc.repo.GetById(id)
}

func (uc *BookUseCase) Update(book *Book) error {
	return uc.repo.Update(book)
}

func (uc *BookUseCase) Delete(id int) error {
	return uc.repo.Delete(id)
}

func (uc *BookUseCase) List() ([]*Book, error) {
	return uc.repo.List()
}

type DynamoDbBookRepository struct {
	client    *dynamodb.Client
	tableName string
}

// Create implements BookRepository.
func (d *DynamoDbBookRepository) Create(book *Book) error {
	av, err := attributevalue.MarshalMap(book)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(d.tableName),
	}
	_, err = d.client.PutItem(context.TODO(), input)
	return err
}

// Delete implements BookRepository.
func (d *DynamoDbBookRepository) Delete(id int) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberN{Value: string(id)},
		},
		TableName: aws.String(d.tableName),
	}
	_, err := d.client.DeleteItem(context.TODO(), input)
	return err
}

// GetById implements BookRepository.
func (d *DynamoDbBookRepository) GetById(id int) (*Book, error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberN{Value: string(id)},
		},
		TableName: aws.String(d.tableName),
	}

	result, err := d.client.GetItem(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	if result.Item == nil {
		return nil, nil
	}
	book := new(Book)
	err = attributevalue.UnmarshalMap(result.Item, book)
	return book, err
}

// List implements BookRepository.
func (d *DynamoDbBookRepository) List() ([]*Book, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(d.tableName),
	}
	result, err := d.client.Scan(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	books := []*Book{}
	err = attributevalue.UnmarshalListOfMaps(result.Items, &books)
	return books, err

}

// Update implements BookRepository.
func (d *DynamoDbBookRepository) Update(book *Book) error {
	av, err := attributevalue.MarshalMap(book)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(d.tableName),
	}
	_, err = d.client.PutItem(context.TODO(), input)
	return err
}

func NewDynamoDBBookRepository(cfg aws.Config, tableName string) BookRepository {
	return &DynamoDbBookRepository{
		client:    dynamodb.NewFromConfig(cfg),
		tableName: tableName,
	}
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-southeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	repo := NewDynamoDBBookRepository(cfg, "book")
	useCase := NewBookUseCase(repo)
	fmt.Println(useCase.List())
	// Print a message to indicate the client was created successfully
	fmt.Println("Successfully created DynamoDB client")
}
