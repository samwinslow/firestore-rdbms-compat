# firestore-rdbms-compat
Firestore &lt;> RDBMS compatibility layer (currently supports EdgeDB, other backends TODO)

# Components

This is a simple interface to **extract** data from Firestore, **transform** it with Pandas and custom rules, and then **load** it into a relational database (currently only EdgeDB is supported).

Requirements:
 - Google Firebase APIs, Places API
 - Pandas for transformation
 - EdgeDB 2.0+ and a DSN to access it

# Declarative transformation syntax

The rules object (defined in `firestore.py`) is a dictionary of transformation rules for each collection.

Each rule has the following keys:

- `is_collection_group`: If true, the collection is a [collection group](https://firebase.blog/posts/2019/06/understanding-collection-group-queries).
- `fetch_order`: A tuple of (field, direction) to order the collection by. Direction is one of `ASCENDING`, `DESCENDING`.
- `mapping`: A dictionary of mappings from Firebase fields to EdgeDB fields. More details below.
- `group_by`: A list of field names to group by.
- `edgedb_iterated_query`: A query to run on each item in the collection, after transforms have been processed.

The `mapping` dict is a dictionary of mappings from Firebase fields to EdgeDB fields. The key of each entry in `mapping` is the name of the output column in EdgeDB.

Mappings can be defined in a few ways:

- `'<output_col>': '<source_field_name>'` Don't apply any transformation.
- `'<output_col>': { 'col': '<source_field_name>', 'transform': <cell_transform_fn> }` Apply a transformation function which takes a single value from the source field and outputs a scalar value.
- `'<output_col>': { 'row': True, 'transform': <row_transform_fn> }` Apply a transformation function which receives the whole row as input and returns a scalar value. This allows you to concatenate or "reduce" multiple columns together.

The `edgedb_iterated_query` is a query which will be run once for each row, taking the transformed data as JSON input. Typically this query will be an insert or update operation.

# TODO

Document CLI options
Document (or remove for now): Resolvers, prerequisites, skip_row_if_empty, type casting

