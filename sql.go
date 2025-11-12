package godbsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/Nemutagk/godb/v2"
	"github.com/Nemutagk/godb/v2/definitions/models"
	"github.com/Nemutagk/godb/v2/definitions/repository"
	"github.com/Nemutagk/goenvars"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const (
	ComparatorEqual              = "="
	ComparatorNotEqual           = "!="
	ComparatorGreaterThan        = ">"
	ComparatorLessThan           = "<"
	ComparatorGreaterThanOrEqual = ">="
	ComparatorLessThanOrEqual    = "<="
	ComparatorLike               = "LIKE"
	ComparatorIn                 = "IN"
	ComparatorNotIn              = "NOT IN"
	ComparatorIsNull             = "IS NULL"
	ComparatorIsNotNull          = "IS NOT NULL"
)

var ErrorNoRows = sql.ErrNoRows

type Model interface {
	ScanFields() []any
}

type Connection[T Model] struct {
	Conn         *sql.DB
	Table        string
	OrderColumns map[string]string
	SoftDelete   *string
	Relationer   Relationer[T]
}

type Relationer[T Model] interface {
	LoadRelations(ctx context.Context, relation string, models []*T) error
}

// Interfaz para que sql.Row y sql.Rows puedan ser usados en la misma función
type Scannable interface {
	Scan(dest ...any) error
}

func scanRow[T Model](row Scannable, model *T) error {
	originalFields := (*model).ScanFields()
	tempFields := make([]any, len(originalFields))
	nullableTimeIndices := make(map[int]*sql.NullTime)

	for i, field := range originalFields {
		// Si el campo es un puntero a time.Time, usamos un sustituto
		if ptr, ok := field.(**time.Time); ok && ptr != nil {
			nt := &sql.NullTime{}
			tempFields[i] = nt
			nullableTimeIndices[i] = nt
		} else {
			tempFields[i] = field
		}
	}

	err := row.Scan(tempFields...)
	if err != nil {
		return fmt.Errorf("failed to scan data: %w", err)
	}

	// Asignar los valores de los sustitutos a los campos originales
	for i, nt := range nullableTimeIndices {
		originalField := originalFields[i].(**time.Time)
		if nt.Valid {
			*originalField = &nt.Time
		} else {
			*originalField = nil
		}
	}

	return nil
}

func NewConnection[T Model](connName, table string, orderColumns []string, softDelete *string, relationer Relationer[T]) (repository.DriverConnection[T], error) {
	db, err := godb.GetConnection(connName)
	if err != nil {
		return nil, err
	}

	rawConn, ok := db.Connection.Adapter.GetConnection().(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("failed to assert connection to *sql.DB")
	}

	orderColsMap := make(map[string]string)
	for _, col := range orderColumns {
		orderColsMap[col] = ""
	}

	return &Connection[T]{
		Conn:         rawConn,
		Table:        table,
		OrderColumns: orderColsMap,
		SoftDelete:   softDelete,
		Relationer:   relationer,
	}, nil
}

func (c *Connection[T]) GetTableName() string {
	return c.Table
}

func (c *Connection[T]) GetOrderColumns() map[string]string {
	return c.OrderColumns
}

func (c *Connection[T]) Get(ctx context.Context, filters models.GroupFilter, opts *models.Options) ([]T, error) {
	if c.SoftDelete != nil && *c.SoftDelete != "" {
		tmpFilters := prepareSoftDelete(c.SoftDelete, filters)
		filters = tmpFilters
	}

	cols := "*"

	if opts != nil && opts.Columns != nil {
		cols = ""
		for i, col := range *opts.Columns {
			if i > 0 {
				cols += ", "
			}
			cols += col
		}
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT ")
	queryBuilder.WriteString(cols)
	queryBuilder.WriteString(" FROM ")
	queryBuilder.WriteString(c.Table)

	args := []any{}

	allFilters, allVals, _ := prepareFilters(filters, 1)
	if allFilters != "" {
		queryBuilder.WriteString(" WHERE ")
		queryBuilder.WriteString(allFilters)
		args = append(args, allVals...)
	}

	if opts != nil {
		if opts.OrderColumn != "" {
			orderDir := "ASC"
			if strings.ToUpper(opts.OrderDir) == "DESC" {
				orderDir = "DESC"
			}

			if _, ok := c.OrderColumns[opts.OrderColumn]; !ok {
				return nil, fmt.Errorf("invalid order column: %s", opts.OrderColumn)
			}

			queryBuilder.WriteString(" ORDER BY " + opts.OrderColumn + " " + orderDir)
		}

		if opts.Limit > 0 {
			queryBuilder.WriteString(" LIMIT " + fmt.Sprintf("%d", opts.Limit))
		}

		if opts.Offset > 0 {
			queryBuilder.WriteString(" OFFSET " + fmt.Sprintf("%d", opts.Offset))
		}
	}

	query := queryBuilder.String()

	if goenvars.GetEnvBool("SQL_DEBUG", false) {
		log.Println("SQL Query:", query)
		log.Println("SQL Args:", args)
	}

	rows, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, godb.ErrNoDocumentsFound
		}
		return nil, err
	}
	defer rows.Close()

	var models []T

	for rows.Next() {
		var newModelT T
		val := reflect.New(reflect.TypeOf(newModelT).Elem())
		newModelT = val.Interface().(T)

		if err := scanRow(rows, &newModelT); err != nil {
			return nil, err
		}

		models = append(models, newModelT)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	if opts != nil && len(opts.Relations) > 0 && c.Relationer != nil {
		modelPointers := make([]*T, len(models))
		for i := range models {
			modelPointers[i] = &models[i]
		}

		for _, relation := range opts.Relations {
			if err := c.Relationer.LoadRelations(ctx, relation, modelPointers); err != nil {
				return nil, fmt.Errorf("failed to load relation '%s': %w", relation, err)
			}
		}
	}

	return models, nil
}

func (c *Connection[T]) GetOne(ctx context.Context, filters models.GroupFilter) (T, error) {
	if c.SoftDelete != nil && *c.SoftDelete != "" {
		tmpFilters := prepareSoftDelete(c.SoftDelete, filters)
		filters = tmpFilters
	}

	opts := &models.Options{
		Limit: 1,
	}

	var zero T

	rows, err := c.Get(ctx, filters, opts)
	if err != nil {
		return zero, err
	}

	if len(rows) == 0 {
		return zero, godb.ErrNoDocumentsFound
	}

	return rows[0], nil
}

func (c *Connection[T]) Create(ctx context.Context, data map[string]any) (T, error) {
	var zero T

	newUuid, err := uuid.NewV7()
	if err != nil {
		return zero, err
	}
	data["id"] = newUuid.String()
	now := time.Now().UTC()
	data["created_at"] = now
	data["updated_at"] = now

	columns := make([]string, 0, len(data))
	values := make([]any, 0, len(data))
	placeholders := make([]string, 0, len(data))
	numItems := 1
	for k, v := range data {
		columns = append(columns, k)
		values = append(values, v)
		placeholders = append(placeholders, fmt.Sprintf("$%d", numItems))
		numItems++
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING *",
		c.Table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	if goenvars.GetEnvBool("SQL_DEBUG", false) {
		log.Println("SQL Query:", query)
		log.Println("SQL Values:", values)
	}

	row := c.Conn.QueryRowContext(ctx, query, values...)

	var modelT T
	val := reflect.New(reflect.TypeOf(modelT).Elem())
	modelT = val.Interface().(T)

	err = scanRow(row, &modelT)
	if err != nil {
		return modelT, err
	}

	return modelT, nil
}

func (c *Connection[T]) Update(ctx context.Context, filters models.GroupFilter, data map[string]any) (T, error) {
	if c.SoftDelete != nil && *c.SoftDelete != "" {
		tmpFilters := prepareSoftDelete(c.SoftDelete, filters)
		filters = tmpFilters
	}

	delete(data, "id")
	delete(data, "created_at")

	data["updated_at"] = time.Now().UTC()

	setParts := make([]string, 0, len(data))
	vals := []any{}
	items := 1
	for k, v := range data {
		setParts = append(setParts, fmt.Sprintf("%s = $%d", k, items))
		vals = append(vals, v)
		items++
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString("UPDATE ")
	queryBuilder.WriteString(c.Table)
	queryBuilder.WriteString(" SET ")
	queryBuilder.WriteString(strings.Join(setParts, ", "))

	allFilters, allVals, _ := prepareFilters(filters, items)
	if allFilters != "" {
		queryBuilder.WriteString(" WHERE ")
		queryBuilder.WriteString(allFilters)
		vals = append(vals, allVals...)
	}

	query := queryBuilder.String()

	if goenvars.GetEnvBool("SQL_DEBUG", false) {
		log.Println("SQL Query:", query)
		log.Println("SQL Values:", vals)
	}

	var zero T

	_, err := c.Conn.ExecContext(ctx, query, vals...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return zero, godb.ErrNoDocumentsFound
		}

		return zero, err
	}

	result, err := c.GetOne(ctx, filters)
	if err != nil {
		return zero, err
	}

	return result, nil
}

func (c *Connection[T]) Delete(ctx context.Context, filters models.GroupFilter) error {
	if c.SoftDelete != nil && *c.SoftDelete != "" {
		c.Update(ctx, filters, map[string]any{
			"deleted_at": time.Now().UTC(),
		})

		return nil
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("DELETE FROM %s", c.Table))

	allFilters, allVals, _ := prepareFilters(filters, 1)
	if allFilters != "" {
		queryBuilder.WriteString(fmt.Sprintf(" WHERE %s", allFilters))
	}

	query := queryBuilder.String()

	if goenvars.GetEnvBool("SQL_DEBUG", false) {
		log.Println("SQL Query:", query)
		log.Println("SQL Values:", allVals)
	}

	result, err := c.Conn.ExecContext(ctx, query, allVals...)
	if err != nil {
		return err
	}

	numRows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if numRows == 0 {
		return godb.ErrNoDocumentsFound
	}

	return nil
}

func (c *Connection[T]) Count(ctx context.Context, filters models.GroupFilter) (int64, error) {
	if c.SoftDelete != nil && *c.SoftDelete != "" {
		tmpFilters := prepareSoftDelete(c.SoftDelete, filters)
		filters = tmpFilters
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("SELECT COUNT(*) FROM %s", c.Table))

	args := []any{}

	allFilters, allVals, _ := prepareFilters(filters, 1)
	if allFilters != "" {
		queryBuilder.WriteString(" WHERE ")
		queryBuilder.WriteString(allFilters)
		args = append(args, allVals...)
	}

	query := queryBuilder.String()

	if goenvars.GetEnvBool("SQL_DEBUG", false) {
		log.Println("SQL Query:", query)
		log.Println("SQL Args:", args)
	}

	var count int64
	err := c.Conn.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func prepareFilters(filters models.GroupFilter, counter int) (string, []any, int) {
	var queryBuilder strings.Builder

	if counter <= 0 {
		counter = 1
	}

	vals := []any{}
	for _, tmpFilter := range filters.Filters {

		var currentParts strings.Builder
		var currentVals []any

		if filter, ok := tmpFilter.(models.Filter); ok {
			comparator := "="
			if filter.Comparator != nil {
				comparator = *filter.Comparator
			}

			if comparator != ComparatorIsNull && comparator != ComparatorIsNotNull && comparator != ComparatorIn && comparator != ComparatorNotIn {
				currentParts.WriteString(fmt.Sprintf("%s %s $%d", filter.Key, comparator, counter))
				currentVals = append(currentVals, filter.Value)

				counter++
			} else if comparator == ComparatorIn || comparator == ComparatorNotIn {
				log.Println("models.Filter not support 'IN' or 'NOT IN' comparator, use models.FilterMultipleValue")
				continue
			} else {
				currentParts.WriteString(fmt.Sprintf("%s %s", filter.Key, comparator))
			}
		} else if multiFilter, ok := tmpFilter.(models.FilterMultipleValue); ok {
			comparator := "IN"
			if multiFilter.Comparator != nil {
				comparator = *multiFilter.Comparator
			}

			if comparator == ComparatorIn || comparator == ComparatorNotIn {
				currentParts.WriteString(fmt.Sprintf("%s %s (", multiFilter.Key, comparator))
				subitems := 1
				for _, v := range multiFilter.Values {
					if subitems > 1 {
						currentParts.WriteString(", ")
					}

					currentParts.WriteString(fmt.Sprintf("$%d", counter))

					currentVals = append(currentVals, v)
					counter++
					subitems++
				}
				currentParts.WriteString(")")
			}
		} else if groupFilter, ok := tmpFilter.(models.GroupFilter); ok {
			subQuery, subVals, newCounter := prepareFilters(groupFilter, counter)
			counter = newCounter

			if subQuery != "" {
				currentParts.WriteString(fmt.Sprintf("(%s)", subQuery))
				currentVals = append(currentVals, subVals...)
			}
		}

		if currentParts.Len() > 0 {
			if queryBuilder.Len() > 0 {
				groupOperator := "AND"
				if filters.Operator != "" {
					groupOperator = filters.Operator
				}
				queryBuilder.WriteString(fmt.Sprintf(" %s ", groupOperator))
			}

			queryBuilder.WriteString(currentParts.String())
			vals = append(vals, currentVals...)
		}
	}

	return queryBuilder.String(), vals, counter
}

func prepareSoftDelete(softDelete *string, filters models.GroupFilter) models.GroupFilter {
	if softDelete == nil || *softDelete == "" {
		return filters
	}

	newGroup := models.GroupFilter{
		Operator: "OR",
		Filters:  []any{},
	}

	// opAnd := models.FilterOperatorAnd
	or := "OR"
	isNull := models.FilterSqlComparatorIsNull

	newGroup.Filters = append(newGroup.Filters, models.Filter{
		Key:        *softDelete,
		Comparator: &isNull,
		Value:      nil,
		Operator:   &or,
	})

	if len(filters.Filters) > 0 {
		filters.Filters = append(filters.Filters, newGroup)
	} else {
		filters = newGroup
	}

	return filters
}
