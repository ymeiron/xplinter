Xplinter
========

![Xplinter logo](logo.png)

Xplinter is a Python library for XML shredding, or the conversion of XML documents into a relational database. 

While document-oriented databases are the right solution in many cases, relational databases are more suitable when the data are highly structured. Sometime highly structured data is given as XML documents, and we want to convert into a traditional relational database (such as PostgreSQL) or even just CSV files.

Xplinter is designed for situations where there are complex relations between entities, that are naturally flattened in the XML source. For example, if an XML node represents a scientific publication, the journal may be represented as a child node of the publication node, the publisher may then be a child of the journal node, and the publisher's city a child of the publisher node. These many-to-many relations can be represented in a database with bridge tables.

In Xplinter, the user defines the database and how to obtain each piece of data using a simple configuration language. The standalone program (TODO) can be used in relatively simple cases to build the database, and in more complicated cases Python can be used to transform data as desired.

This application is under development and not ready for public release.

Terminology
-----------

**Datum** A datum is any piece of information in the *output*.

**Generator** A generator is a specification on how to generate a datum from the XML source or other data.

**Field** A field, or more correctly, *field descriptor*, is an object that keeps the metadata on a particular piece of information to extract from the XML. It contains the name (which will be the column name in a table in the output), type (as in string, integer, etc.), and one or more generators. The field object does not contain the actual data.

**Entity** An entity is a collection of fields **and** the data generated after processing. It is generally equivalent to a table in the output. 


-------------


When processing the XML tree, an entity extract the relevant node and passes them to the fields for further processing. If there are multiple nodes (see *cardinality* below) then the entity loops over them such that only a single node is passed to the fields at a time. An entity is either a *root entity* (see below) or it has a parent

**Cardinality** refers to the number of nodes in an XML tree that correspond to an entity. An entity with cardinality of *one* expects exactly one node, an entity with cardinality of *zero or one* expect either zero or one node, and so on for *one or more* and *zero or more*.

**Root entity** cardinality = 1

**View** A view corresponds to a table in the output. It is often convenient to create *hidden* entities by having their names start with a star (\*). A view can be used to create a table in the output from selected fields in a hidden (or not) entity. It is often useful to have one big entity with many fields (usually the root entity) and then create several views of that entity by grouping fields as needed.

**Record** TODO

**KV Entity** In a normal entity, the data fields corresponds to the table columns, that is the field name is the column name and the data are the column elements. In this special entity, the data fields' names and their data appear as key-value pairs in two columns. The entity may contain some additional auxiliary fields.


Configuration language syntax
-----------------------------

