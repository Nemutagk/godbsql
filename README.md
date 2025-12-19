# godb-sql — Driver SQL para godb

Resumen
- godb-sql provee una implementación de driver SQL (Postgres compatible) para el proyecto `godb`.
- Soporta operaciones CRUD, filtros avanzados, expresiones SQL raw y carga de relaciones (1:1, 1:N, N:M) con encadenamiento (p.ej. `user.roles.permissions`) de profundidad N.

Requisitos
- Go 1.20+ (u otra versión compatible con tu proyecto)
- Base de datos PostgreSQL (u otro adaptador compatible)
- El proyecto usa internamente:
  - github.com/Nemutagk/godb/v2
  - github.com/Nemutagk/godb/v2/definitions/models
  - github.com/Nemutagk/godb/v2/definitions/repository

Instalación
- Añade el módulo a tu go.mod (si el paquete estuviera publicado) o úsalo como módulo local dentro de tu monorepo.

Importar
```go
import (
    "context"
    "github.com/Nemutagk/godb/v2/definitions/models"
    "github.com/Nemutagk/godb-sql" // ajustar al path real del módulo si difiere
)
```

Inicializar una conexión (ejemplo)
- `NewConnection` devuelve un `repository.DriverConnection[T]` listo para usar en los repositorios de dominio.

```go
// ejemplo conceptual
conn, err := godbsql.NewConnection[YourModel](
    "main-db",            // connName (se resuelve vía godb.GetConnection)
    "your_table",         // table
    []string{"id","name"},// orderColumns permitidos
    nil,                  // softDelete column name pointer o nil
    map[string]repository.RelationLoader{ // relacioners del modelo
        "roles": &godbsql.ManyToManyLoader[*User, *Role]{
            Repository:      roleRepoConnection, // repository.DriverConnection[*Role]
            Connection:      sqlDB,              // *sql.DB o lo que uses internamente
            ParentKey:       "Id",
            ChildKey:        "Id",
            PivoteParentKey: "user_id",
            PivoteChildKey:  "role_id",
            PivoteTable:     "roles_users",
            ContainerField:  "Roles",
        },
        "profile": &godbsql.OnetoOneLoader[*User, *Profile]{
            Repository:     profileRepoConnection,
            ParentField:    "Id",
            ChildFkField:   "user_id",
            ContainerField: "Profile",
        },
    },
)
```

Modelos y Options
- Los modelos deben implementar la interfaz `Model` usada por la librería (p.ej. proveer `ScanFields()`).
- `models.Options` se usa para pasar opciones a `Get`/`GetOne`, ejemplo de campos:
  - Relations []string — relaciones a cargar (soporta notación con punto: `user.roles.permissions`)
  - Columns []string — columnas a retornar (si no se define, se usan todas las columnas)
  - Limit, Offset, OrderColumn, OrderDir, PrimaryKey, InsertPrimaryKey, etc.

Uso básico: Get / GetOne
```go
opts := &models.Options{
    Limit:  10,
    Offset: 0,
}

filters := models.GroupFilter{
    Filters: []any{
        models.Filter{Key: "status", Value: "active"},
    },
}

rows, err := conn.Get(ctx, filters, opts)
one, err := conn.GetOne(ctx, filters, opts)
```

Filtros avanzados
- `models.GroupFilter` puede mezclar filtros simples, múltiples valores y grupos lógicos.
- Ejemplo con IN:
```go
in := godbsql.ComparatorIn
filters := models.GroupFilter{
    Filters: []any{
        models.FilterMultipleValue{
            Key: "id",
            Values: []any{1,2,3},
            Comparator: &in,
        },
    },
}

isNull := godbsql.ComparatorIsNull
greaterThan := godbsql.ComparatorGreaterThan
or := godbsql.OperatorOr
filters := models.GroupFilter{
    Filters: []any{
        models.Filter{
            Key: "id",
            Values: 2,
        },
        models.GrpupFilter{
          Filters: []any{
            models.Filter{Key: "expires_at", Comparator: &isNull},
            models.Filter{Key: "expires_at", Value: time.Now().UTC(), Comparator: &greaterThan}
          }
          Operator: or
        }
    },
}
```

Create / CreateMany
```go
// Create
data := map[string]any{
    "name": "example",
    "age":  30,
}
created, err := conn.Create(ctx, data, nil)

// CreateMany (lista de mapas)
items := []map[string]any{
  {"name":"a","age":20},
  {"name":"b","age":21},
}
createdList, err := conn.CreateMany(ctx, items, nil)
```

Update con expresiones raw
- Para soportar expresiones (p.ej. `position = position + 1`) la librería reconoce un wrapper `RawSQL`:
```go
type RawSQL string
func (r RawSQL) String() string { return string(r) }
```
- Uso:
```go
data := map[string]any{
    "position": RawSQL("position + 1"),
    "name":     "new",
}
updated, err := conn.Update(ctx, filters, data, nil)
```
- Seguridad: raw SQL se inyecta tal cual — riesgo de SQL injection si contiene datos no confiables. Usar solo expresiones generadas por código o validadas.

Delete / Soft delete
- Si la conexión se creó con `softDelete` (nombre de columna), `Delete` puede marcar en vez de eliminar físicamente según tu implementación. Errores ahora se reportan correctamente cuando falla soft delete.

Queries raw
- Puedes construir queries manuales usando `c.Conn.QueryContext(...)` si necesitas total control.
- Para operaciones dentro de la librería, usa `RawSQL` para valores literales en `Create`/`Update`.

Relaciones y encadenamiento (soporta N niveles)
- Las relaciones se pasan en `models.Options.Relations` usando notación punto:
  - Ejemplo: `Relations: []string{"user.roles.permissions"}`
- Comportamiento:
  - Cada nivel solo resuelve su segmento directo y pasa la subruta restante (como una sola cadena con puntos) al repo hijo.
  - Ejemplo real:
    - `session` loader recibe `"user.roles.permissions"` → carga `user` y pasa `"roles.permissions"` al repo `User`.
    - `user` loader recibe `"roles.permissions"` → carga `roles` y pasa `"permissions"` al repo `Role`.
    - `role` loader recibe `"permissions"` → carga `permissions`.
- Tipos de loaders:
  - OnetoOneLoader[P,C]
  - OnetoManyLoader[P,C]
  - ManyToManyLoader[P,C]
- Los loaders deben registrar sus nombres en el mapa `RelationLoaders` al crear la `Connection` del modelo.

Ejemplo de llamada con relaciones:
```go
opts := &models.Options{
    Relations: []string{"user.roles.permissions"},
}
sessions, err := sessionRepo.Get(ctx, filters, opts)
```

Columnas selectivas (Columns)
- `opts.Columns = []string{"id","name"}` permite especificar columnas retornadas.
- La función `scanRow` maneja campos direccionables y placeholders para evitar fallos cuando se solicitan columnas parciales.

MockRepository para tests
- En `testing` hay un `MockRepository[T]` para pruebas unitarias. Se puede usar para simular `GetConnection`, `Get`, etc.

Depuración y logs
- Habilita la variable de entorno `SQL_DEBUG=true` para que la librería imprima queries y argumentos en logs.

Buenas prácticas y notas
- Asegúrate de registrar correctamente `RelationLoaders` por cada repositorio (cada Connection[T] tiene su propio mapa).
- Usa la notación con puntos para encadenar relaciones; la librería reparte la ruta correctamente por niveles.
- Ten cuidado con `RawSQL` — no pasar input de usuario sin sanitizar.
- Verifica que cada `connName` pase la configuración correcta a `godb.GetConnection` para evitar reusar adapters por error.

Ejemplos adicionales y tests sugeridos
- Tests que cubran:
  - `user.roles.permissions`
  - `session.user.roles.permissions`
  - Create/Update con `RawSQL`
  - `Get` con `opts.Columns` parciales
- Añadir pruebas unitarias con `MockRepository` para validar comportamiento de loaders sin DB.

Contacto / Contribución
- Documentar en código la creación de cada `RelationLoader` para que sea fácil reutilizar en múltiples repositorios.
- Abrir issues/PRs con casos reproducibles si encuentras comportamientos inesperados.
