Xplinter
========

![Xplinter logo](logo.png)

Xplinter is a Python library for XML shredding, or the conversion of XML documents into a relational database. 

While document-oriented databases are the right solution in many cases, relational databases are more suitable when the data are highly structured. Sometime highly structured data is given as XML documents, and we want to convert into a traditional relational database (such as PostgreSQL) or even just CSV files. Xplinter is designed for situations where there are complex relations between entities, that are naturally flattened in the XML source.

In Xplinter, the user defines the database and how to obtain each piece of data using a simple configuration language. The standalone program (TODO) can be used in relatively simple cases to build the database, and in more complicated cases Python can be used to transform data as desired.

This application is under development and not ready for public release.

Terminology
-----------

**Datum** A datum is any piece of information in the *output*.

**Generator** A generator is a specification on how to generate a datum from the XML source or other data.

**Field** A field, or more correctly, *field descriptor*, is an object that keeps the metadata on a particular piece of information to extract from the XML. It contains the name (which will be the column name in a table in the output), type (as in string, integer, etc.), and one or more generators. The field object does not contain the actual data.

**Entity** An entity is a collection of fields **and** the data generated after processing. It is generally equivalent to an XML node (or nodes) in the input and a table in the output. An entity is either the *root entity* (see below) or it has a parent. Fields whose name start with a star (\*) are *hidden*, i.e. not included as columns in the output table. Similarly the entity itself is hidden if its name starts with a star, i.e. it does not produce an output table (but see *view* below).

**Cardinality** refers to the number of nodes in an XML tree that correspond to an entity. An entity with cardinality of *one* expects exactly one node, an entity with cardinality of *zero or one* expect either zero or one node, and so on for *one or more* and *zero or more*.

**Root entity** is an entity that corresponds to the root node of the XML source, it has cardinality of one.

**View** A view has an associated entity and corresponds to a table in the output. A view specifies which fields in the associated entity to include as columns (the fields and/or associated entity may or may not be hidden, but they become visible in the view). It is often useful to have one big entity with many fields (usually the root entity) and then create several views of that entity by grouping fields as needed.

**KV Entity** In a normal entity, the data fields corresponds to the table columns, that is the field name is the column name and the data are the column elements. In this special entity, the data fields' names and their data appear as key-value pairs in two columns. The KV entity may contain some additional auxiliary fields.

**Record** The full collection of entities and views. The complete description of how to process a single XML source document.

Configuration language syntax
-----------------------------

Example
-------

For example, if an XML node represents a scientific publication, the journal may be represented as a child node of the publication node, the publisher may then be a child of the journal node, and the publisher's city a child of the publisher node. These many-to-many relations can be represented in a database with bridge tables.