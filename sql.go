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
	OperatorAnd                  = "AND"
	OperatorOr                   = "OR"
)

var ErrorNoRows = sql.ErrNoRows

type Model interface {
	ScanFields() []any
}

type OnetoManyLoader[P Model, C Model] struct {
	Repository     repository.Repository[C]
	ParentField    string
	ChildFkField   string
	ContainerField string
}

type ManyToManyLoader[P Model, C Model] struct {
	Repository      repository.Repository[C]
	Connection      any
	ParentKey       string
	ChildKey        string
	PivoteParentKey string
	PivoteChildKey  string
	PivoteTable     string
	ContainerField  string
}

type OnetoOneLoader[P Model, C Model] struct {
	Repository     repository.Repository[C]
	ParentField    string
	ChildFkField   string
	ContainerField string
}

type Connection[T Model] struct {
	Conn            *sql.DB
	Table           string
	OrderColumns    map[string]string
	SoftDelete      *string
	RelationLoaders map[string]repository.RelationLoader
}

// Interfaz para que sql.Row y sql.Rows puedan ser usados en la misma función
type Scannable interface {
	Scan(dest ...any) error
}

func (l *OnetoManyLoader[P, C]) Load(ctx context.Context, parentModels []any, childs *[]string) error {
	if len(parentModels) == 0 {
		return nil
	}

	parentIds := make([]any, 0, len(parentModels))
	for _, model := range parentModels {
		val := reflect.ValueOf(model)

		for val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		parentIdField := val.FieldByName(l.ParentField)
		if !parentIdField.IsValid() {
			return fmt.Errorf("invalid parent field: %s", l.ParentField)
		}
		parentId := parentIdField.Interface()
		parentIds = append(parentIds, parentId)
	}

	in := ComparatorIn
	filters := models.GroupFilter{
		Filters: []any{
			models.FilterMultipleValue{
				Key:        l.ChildFkField,
				Values:     parentIds,
				Comparator: &in,
			},
		},
	}

	opts := models.Options{}

	if childs != nil && len(*childs) > 0 {
		opts.Relations = *childs
	}

	allChildrens, err := l.Repository.Get(ctx, filters, &opts)
	if err != nil {
		return fmt.Errorf("failed to get child models: %w", err)
	}

	for _, child := range allChildrens {
		valForFieldAcces := reflect.ValueOf(child)
		for valForFieldAcces.Kind() == reflect.Ptr {
			valForFieldAcces = valForFieldAcces.Elem()
		}

		foreignKeyTmp := prepareForeignKey(l.ChildFkField)
		childFkValue := valForFieldAcces.FieldByName(foreignKeyTmp)
		if !childFkValue.IsValid() {
			return fmt.Errorf("invalid child foreign key field: %s, %s", foreignKeyTmp, l.ChildFkField)
		}

		foreignKey := childFkValue.Interface()

		for _, parent := range parentModels {
			parentVal := reflect.ValueOf(parent)
			for parentVal.Kind() == reflect.Ptr {
				parentVal = parentVal.Elem()
			}

			parentIdField := parentVal.FieldByName(l.ParentField)
			if !parentIdField.IsValid() {
				return fmt.Errorf("invalid parent field: %s", l.ParentField)
			}
			parentId := parentIdField.Interface()

			if foreignKey != parentId {
				continue
			}

			containerField := parentVal.FieldByName(l.ContainerField)
			if !containerField.IsValid() {
				return fmt.Errorf("invalid container field: %s", l.ContainerField)
			}

			elemToAppend := valForFieldAcces
			if containerField.Type().Elem().Kind() != reflect.Ptr && elemToAppend.Kind() == reflect.Ptr {
				elemToAppend = elemToAppend.Elem()
			} else if containerField.Type().Elem().Kind() == reflect.Ptr && elemToAppend.Kind() != reflect.Ptr {
				ptr := reflect.New(elemToAppend.Type())
				ptr.Elem().Set(elemToAppend)
				elemToAppend = ptr
			}

			switch containerField.Kind() {
			case reflect.Slice:
				containerField.Set(reflect.Append(containerField, elemToAppend))
			case reflect.Ptr:
				if containerField.Type().Elem().Kind() != reflect.Slice {
					return fmt.Errorf("container field pointer is not pointing to a slice: %s", l.ContainerField)
				}

				if containerField.IsNil() {
					sliceType := containerField.Type().Elem()
					emptySlce := reflect.MakeSlice(sliceType, 0, 0)
					ptr := reflect.New(sliceType)
					ptr.Elem().Set(emptySlce)
					containerField.Set(ptr)
				}

				sliceVal := containerField.Elem()
				sliceVal = reflect.Append(sliceVal, elemToAppend)
				containerField.Elem().Set(sliceVal)
			default:
				return fmt.Errorf("container field is not a slice or pointer to slice: %s", l.ContainerField)
			}
		}
	}

	return nil
}

func (m *ManyToManyLoader[P, C]) Load(ctx context.Context, parentModels []any, childs *[]string) error {
	if len(parentModels) == 0 {
		return nil
	}

	parentModelsIds := []any{}
	for _, model := range parentModels {
		val := reflect.ValueOf(model)
		for val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		parentIdField := val.FieldByName(m.ParentKey)
		if !parentIdField.IsValid() {
			return fmt.Errorf("invalid parent key field: %s", m.ParentKey)
		}

		parentModelsIds = append(parentModelsIds, parentIdField.Interface())
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString("SELECT ")
	queryBuilder.WriteString(m.PivoteParentKey)
	queryBuilder.WriteString(", ")
	queryBuilder.WriteString(m.PivoteChildKey)
	queryBuilder.WriteString(" FROM ")
	queryBuilder.WriteString(m.PivoteTable)
	queryBuilder.WriteString(" WHERE ")
	queryBuilder.WriteString(m.PivoteParentKey)
	queryBuilder.WriteString(" IN (")

	for i := range parentModelsIds {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString(fmt.Sprintf("$%d", i+1))
	}
	queryBuilder.WriteString(")")

	query := queryBuilder.String()
	if goenvars.GetEnvBool("SQL_DEBUG", false) {
		log.Println("SQL Query:", query)
		log.Println("SQL Values:", parentModelsIds)
	}

	sqlConn, ok := m.Connection.(*sql.DB)
	if !ok {
		return fmt.Errorf("failed to assert connection to *sql.DB")
	}

	rows, err := sqlConn.QueryContext(ctx, query, parentModelsIds...)
	if err != nil {
		return fmt.Errorf("failed to query pivot table: %w", err)
	}
	defer rows.Close()

	listChildForParent := map[any][]any{}
	listAllChildIds := []any{}
	for rows.Next() {
		var parentId, childId any

		if err := rows.Scan(&parentId, &childId); err != nil {
			return fmt.Errorf("failed to scan pivot row: %w", err)
		}

		if _, exists := listChildForParent[childId]; !exists {
			listChildForParent[childId] = []any{}
		}

		listChildForParent[childId] = append(listChildForParent[childId], parentId)
		listAllChildIds = append(listAllChildIds, childId)
	}

	in := ComparatorIn
	filtersForChildren := models.GroupFilter{
		Filters: []any{
			models.FilterMultipleValue{
				Key:        m.ChildKey,
				Values:     listAllChildIds,
				Comparator: &in,
			},
		},
	}

	opts := models.Options{}

	if childs != nil && len(*childs) > 0 {
		opts.Relations = *childs
	}

	allChildren, err := m.Repository.Get(ctx, filtersForChildren, &opts)
	if err != nil {
		return fmt.Errorf("failed to get child models: %w", err)
	}

	finalContainerChilds := map[string][]any{}
	for _, child := range allChildren {
		valForFieldAcces := reflect.ValueOf(child)
		for valForFieldAcces.Kind() == reflect.Ptr {
			valForFieldAcces = valForFieldAcces.Elem()
		}

		childKeyField := valForFieldAcces.FieldByName(m.ChildKey)
		if !childKeyField.IsValid() {
			return fmt.Errorf("invalid child key field: %s", m.ChildKey)
		}

		childKeyValue := childKeyField.Interface()

		parentIdsForChild, exists := listChildForParent[childKeyValue]
		if !exists {
			log.Println("No parent IDs found for child key value:", childKeyValue)
			continue
		}

		for _, parentId := range parentIdsForChild {
			_, exists := finalContainerChilds[fmt.Sprintf("%v", parentId)]
			if !exists {
				finalContainerChilds[fmt.Sprintf("%v", parentId)] = []any{}
			}

			elemToAppend := valForFieldAcces
			if reflect.TypeOf(finalContainerChilds[fmt.Sprintf("%v", parentId)]).Elem().Kind() != reflect.Ptr && elemToAppend.Kind() == reflect.Ptr {
				elemToAppend = elemToAppend.Elem()
			} else if reflect.TypeOf(finalContainerChilds[fmt.Sprintf("%v", parentId)]).Elem().Kind() == reflect.Ptr && elemToAppend.Kind() != reflect.Ptr {
				ptr := reflect.New(elemToAppend.Type())
				ptr.Elem().Set(elemToAppend)
				elemToAppend = ptr
			}

			finalContainerChilds[fmt.Sprintf("%v", parentId)] = append(finalContainerChilds[fmt.Sprintf("%v", parentId)], elemToAppend.Interface())
		}
	}

	// Asignar los hijos agrupados a cada padre
	for _, parent := range parentModels {
		// parent viene como interface{}, normalmente *app.User o similar
		pv := reflect.ValueOf(parent)

		// Desenvolver interfaces
		for pv.Kind() == reflect.Interface {
			pv = pv.Elem()
		}

		// Necesitamos un valor direccionable del padre
		var parentPtr reflect.Value
		if pv.Kind() == reflect.Ptr {
			parentPtr = pv
		} else if pv.Kind() == reflect.Struct {
			if !pv.CanAddr() {
				return fmt.Errorf("parent value is not addressable")
			}
			parentPtr = pv.Addr()
		} else {
			return fmt.Errorf("unexpected parent kind: %s", pv.Kind())
		}

		// Valor struct para leer campos: desenrollar todos los punteros
		parentVal := parentPtr
		for parentVal.Kind() == reflect.Ptr {
			parentVal = parentVal.Elem()
		}

		if parentVal.Kind() != reflect.Struct {
			return fmt.Errorf("expected struct for parent, got: %s", parentVal.Kind())
		}

		// 1) Obtener ID del padre
		parentIdField := parentVal.FieldByName(m.ParentKey)
		if !parentIdField.IsValid() {
			return fmt.Errorf("invalid parent key field: %s", m.ParentKey)
		}
		parentKeyValue := fmt.Sprintf("%v", parentIdField.Interface())

		// 2) Buscar hijos en el mapa
		childs, ok := finalContainerChilds[parentKeyValue]
		if !ok {
			continue // este padre no tiene hijos
		}

		// 3) Obtener el campo contenedor en el padre
		containerField := parentVal.FieldByName(m.ContainerField)
		if !containerField.IsValid() {
			return fmt.Errorf("invalid container field: %s", m.ContainerField)
		}

		var sliceType reflect.Type

		switch containerField.Kind() {
		case reflect.Slice:
			// Campo es directamente []T
			sliceType = containerField.Type()
		case reflect.Ptr:
			// Campo es *([]T)
			if containerField.Type().Elem().Kind() != reflect.Slice {
				return fmt.Errorf("container field %s is a ptr but not to slice, got: %s",
					m.ContainerField, containerField.Type().Elem().Kind())
			}

			// Si el puntero es nil, creamos un slice nuevo vacío y se lo asignamos
			if containerField.IsNil() {
				sliceType = containerField.Type().Elem() // []T
				emptySlice := reflect.MakeSlice(sliceType, 0, len(childs))
				ptr := reflect.New(sliceType) // *([]T)
				ptr.Elem().Set(emptySlice)
				containerField.Set(ptr)
			}

			// A partir de aquí, siempre trabajamos con el tipo del slice subyacente
			sliceType = containerField.Type().Elem()
		default:
			return fmt.Errorf("container field %s is not a slice, got: %s",
				m.ContainerField, containerField.Kind())
		}

		elemType := sliceType.Elem()

		// 4) Crear slice del tipo correcto
		newSlice := reflect.MakeSlice(sliceType, 0, len(childs))

		// 5) Convertir cada hijo al tipo de elemento del slice
		for _, c := range childs {
			cv := reflect.ValueOf(c)

			if elemType.Kind() == reflect.Ptr {
				if cv.Kind() != reflect.Ptr {
					if cv.Type() != elemType.Elem() {
						return fmt.Errorf("child type %s does not match container element type %s",
							cv.Type(), elemType)
					}
					ptr := reflect.New(cv.Type())
					ptr.Elem().Set(cv)
					cv = ptr
				} else {
					if cv.Type() != elemType {
						return fmt.Errorf("child pointer type %s does not match container element type %s",
							cv.Type(), elemType)
					}
				}
			} else {
				if cv.Kind() == reflect.Ptr {
					cv = cv.Elem()
				}
				if cv.Type() != elemType {
					return fmt.Errorf("child value type %s does not match container element type %s",
						cv.Type(), elemType)
				}
			}

			newSlice = reflect.Append(newSlice, cv)
		}

		// 6) Asignar el slice al padre
		switch containerField.Kind() {
		case reflect.Slice:
			containerField.Set(newSlice) // []T = []T
		case reflect.Ptr:
			containerField.Elem().Set(newSlice) // *([]T) -> []T
		}
	}

	return nil
}

func (c *OnetoOneLoader[P, C]) Load(ctx context.Context, parentModel []any, childs *[]string) error {
	if parentModel == nil || len(parentModel) == 0 {
		return nil
	}

	val := reflect.ValueOf(parentModel[0])

	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	parentIdField := val.FieldByName(c.ParentField)
	if !parentIdField.IsValid() {
		return fmt.Errorf("invalid parent field: %s", c.ParentField)
	}
	parentId := parentIdField.Interface()

	filterChild := models.GroupFilter{
		Filters: []any{
			models.Filter{Key: c.ChildFkField, Value: parentId},
		},
	}

	opts := models.Options{}

	if childs != nil && len(*childs) > 0 {
		opts.Relations = *childs
	}

	childModel, err := c.Repository.GetOne(ctx, filterChild, &opts)
	if err != nil {
		return fmt.Errorf("failed to load child model: %w", err)
	}

	containerFlied := val.FieldByName(c.ContainerField)
	if !containerFlied.IsValid() {
		return fmt.Errorf("invalid container field: %s", c.ContainerField)
	}

	if !containerFlied.CanSet() {
		return fmt.Errorf("cannot set container field: %s", c.ContainerField)
	}

	childVal := reflect.ValueOf(childModel)

	switch containerFlied.Kind() {
	case reflect.Ptr:
		if childVal.Kind() != reflect.Ptr {
			if childVal.Type() != containerFlied.Type().Elem() {
				return fmt.Errorf("child type %s does not match container field type %s",
					childVal.Type(), containerFlied.Type().Elem())
			}
			ptr := reflect.New(childVal.Type())
			ptr.Elem().Set(childVal)
			childVal = ptr
		} else {
			if childVal.Type() != containerFlied.Type() {
				return fmt.Errorf("child pointer type %s does not match container field type %s",
					childVal.Type(), containerFlied.Type())
			}
		}
		containerFlied.Set(childVal)
	case reflect.Interface:
		if childVal.Type().Implements(containerFlied.Type()) {
			containerFlied.Set(childVal)
		} else {
			return fmt.Errorf("child type %s does not implement container interface type %s",
				childVal.Type(), containerFlied.Type())
		}
	default:
		if childVal.Kind() == reflect.Ptr {
			childVal = childVal.Elem()
		}
		if childVal.Type() != containerFlied.Type() {
			return fmt.Errorf("child value type %s does not match container field type %s",
				childVal.Type(), containerFlied.Type())
		}
		containerFlied.Set(childVal)
	}

	return nil
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

func NewConnection[T Model](connName, table string, orderColumns []string, softDelete *string, relationer map[string]repository.RelationLoader) (repository.DriverConnection[T], error) {
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
		Conn:            rawConn,
		Table:           table,
		OrderColumns:    orderColsMap,
		SoftDelete:      softDelete,
		RelationLoaders: relationer,
	}, nil
}

func (c *Connection[T]) GetTableName() string {
	return c.Table
}

func (c *Connection[T]) GetOrderColumns() map[string]string {
	return c.OrderColumns
}

func (c *Connection[T]) GetConnection() any {
	return c.Conn
}

func (c *Connection[T]) AddRelation(relation string, loader repository.RelationLoader) error {
	if c.RelationLoaders == nil {
		c.RelationLoaders = make(map[string]repository.RelationLoader)
	}

	if _, exists := c.RelationLoaders[relation]; exists {
		return fmt.Errorf("relation loader already exists for relation: %s", relation)
	}

	c.RelationLoaders[relation] = loader
	return nil
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

	if opts != nil && len(opts.Relations) > 0 && c.RelationLoaders != nil {
		modelPointers := make([]*T, len(models))
		for i := range models {
			modelPointers[i] = &models[i]
		}

		anyModels := make([]any, len(modelPointers))
		for i, m := range modelPointers {
			anyModels[i] = m
		}

		for _, relation := range opts.Relations {
			childs := &[]string{}
			if strings.Contains(relation, ".") {
				tmp_items := strings.Split(relation, ".")
				relation = tmp_items[0]
				*childs = tmp_items[1:]
			}

			loader, ok := c.RelationLoaders[relation]
			if !ok {
				return nil, fmt.Errorf("relation loader not found for relation: %s", relation)
			}

			if err := loader.Load(ctx, anyModels, childs); err != nil {
				return nil, fmt.Errorf("failed to load relation %s: %w", relation, err)
			}
		}
	}

	return models, nil
}

func (c *Connection[T]) GetOne(ctx context.Context, filters models.GroupFilter, opts *models.Options) (T, error) {
	if opts == nil {
		opts = &models.Options{
			Limit: 1,
		}
	} else {
		opts.Limit = 1
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

func (c *Connection[T]) Create(ctx context.Context, data map[string]any, opts *models.Options) (T, error) {
	var zero T

	newUuid, err := uuid.NewV7()
	if err != nil {
		return zero, err
	}

	if opts == nil {
		opts = &models.Options{}
	}

	if opts.PrimaryKey == nil {
		idStr := "id"
		opts.PrimaryKey = &idStr
	}

	if opts.InsertPrimaryKey == nil {
		insertPk := true
		opts.InsertPrimaryKey = &insertPk
	}

	if opts.TimestampsFields == nil {
		timestamps := true
		opts.TimestampsFields = &timestamps
	}

	if *opts.InsertPrimaryKey {
		data[*opts.PrimaryKey] = newUuid.String()
	}

	if *opts.TimestampsFields {
		now := time.Now().UTC()
		data["created_at"] = now
		data["updated_at"] = now
	}

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

func (c *Connection[T]) CreateMany(ctx context.Context, dataList []map[string]any, opts *models.Options) ([]T, error) {
	if len(dataList) == 0 {
		return []T{}, nil
	}

	data := dataList[0]
	columns := make([]string, 0, len(data))
	for k := range data {
		columns = append(columns, k)
	}

	var query strings.Builder
	// query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
	query.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		c.Table,
		strings.Join(columns, ", "),
	))

	values := make([]any, 0, len(data))
	totalItems := 1
	for indexData, tmpItem := range dataList {
		placeholders := make([]string, 0, len(data))

		if indexData > 0 {
			query.WriteString(", ")
		}

		for _, k := range tmpItem {
			values = append(values, k)
			placeholders = append(placeholders, fmt.Sprintf("$%d", totalItems))
			totalItems++
		}

		query.WriteString(fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	queryStr := query.String() + " RETURNING *"
	if goenvars.GetEnvBool("SQL_DEBUG", false) {
		log.Println("SQL Query:", queryStr)
		log.Println("SQL Values:", values)
	}

	rows, err := c.Conn.QueryContext(ctx, queryStr, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resultModels []T
	for rows.Next() {
		var modelT T
		val := reflect.New(reflect.TypeOf(modelT).Elem())
		modelT = val.Interface().(T)

		if err := scanRow(rows, &modelT); err != nil {
			return nil, err
		}

		resultModels = append(resultModels, modelT)
	}

	return resultModels, nil
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

	result, err := c.GetOne(ctx, filters, nil)
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
			// log.Printf("queryBuilderLen: %d\n", queryBuilder.Len())
			if queryBuilder.Len() > 0 {
				groupOperator := OperatorAnd
				if filters.Operator != "" {
					groupOperator = filters.Operator
				}
				// log.Printf("Using group operator: %s\n", groupOperator)
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

func prepareForeignKey(str string) string {
	// return strings.ToUpper(str[:1]) + str[1:]

	parts := strings.Split(str, "_")

	buffer := strings.Builder{}

	for i := range parts {
		// if i > 0 {
		// 	buffer.WriteString("_")
		// }

		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		buffer.WriteString(parts[i])
	}

	return buffer.String()
}
