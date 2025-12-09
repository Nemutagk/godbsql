package testing

import (
	"context"

	"github.com/Nemutagk/godb/v2/definitions/models"
	"github.com/Nemutagk/godb/v2/definitions/repository"
	"github.com/stretchr/testify/mock"
)

type MockRepository[T any] struct {
	mock.Mock
}

func NewMockRepository[T any]() MockRepository[T] {
	return MockRepository[T]{}
}

func (m *MockRepository[T]) GetTableName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockRepository[T]) GetOrderColumns() map[string]string {
	args := m.Called()
	return args.Get(0).(map[string]string)
}

func (m *MockRepository[T]) GetConnection() any {
	args := m.Called()
	return args.Get(0)
}

func (m *MockRepository[T]) AddRelation(name string, loader repository.RelationLoader) error {
	args := m.Called(name, loader)
	return args.Error(0)
}

func (m *MockRepository[T]) Get(ctx context.Context, filters models.GroupFilter, opts *models.Options) ([]T, error) {
	args := m.Called(ctx, filters, opts)
	return args.Get(0).([]T), args.Error(1)
}

func (m *MockRepository[T]) GetOne(ctx context.Context, filters models.GroupFilter, opts *models.Options) (T, error) {
	args := m.Called(ctx, filters, opts)
	return args.Get(0).(T), args.Error(1)
}

func (m *MockRepository[T]) Create(ctx context.Context, payload map[string]any, opts *models.Options) (T, error) {
	args := m.Called(ctx, payload, opts)
	return args.Get(0).(T), args.Error(1)
}

func (m *MockRepository[T]) CreateMany(ctx context.Context, payloads []map[string]any, opts *models.Options) ([]T, error) {
	args := m.Called(ctx, payloads, opts)
	return args.Get(0).([]T), args.Error(1)
}

func (m *MockRepository[T]) Update(ctx context.Context, filters models.GroupFilter, payload map[string]any, opts *models.Options) (T, error) {
	args := m.Called(ctx, filters, payload)
	return args.Get(0).(T), args.Error(1)
}

func (m *MockRepository[T]) Delete(ctx context.Context, filters models.GroupFilter) error {
	args := m.Called(ctx, filters)
	return args.Error(0)
}

func (m *MockRepository[T]) Count(ctx context.Context, filters models.GroupFilter) (int64, error) {
	args := m.Called(ctx, filters)
	return args.Get(0).(int64), args.Error(1)
}
