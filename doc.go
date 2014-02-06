/*
Package ibis provides a higher-level interface to the wonderful github.com/gocql/gocql package.

Schemas

In ibis, a Schema represents the definition of a set of column families in a Cassandra keyspace.
For certain simple cases, ibis can also automate the creation of these column families, or the
addition of new columns at a later date.

        userCols := []*ibis.Column{
            &ibis.Column{Name: "User", Type: "varchar"},
            &ibis.Column{Name: "Password", Type: "varchar"},
            &ibis.Column{Name: "CreatedAt", Type: "timestamp"},
        }
        schema := ibis.NewSchema()
        schema.AddCF(&ibis.CF{Name: "users", Columns: userCols})

To reduce boilerplate, ibis offers reflection. For example, to generate a schema definition from
a struct of column families:

        userCols := []*ibis.Column{
            &ibis.Column{Name: "User", Type: "varchar"},
            &ibis.Column{Name: "Password", Type: "varchar"},
            &ibis.Column{Name: "CreatedAt", Type: "timestamp"},
        }
        type Model struct{Users *ibis.CF}
        model := &Model{Users: &ibis.CF{Columns: userCols}}
        schema := ibis.ReflectSchema(model)

Reflection is also available for defining column families themselves:

        type User struct {Name string, Password string, CreatedAt time.Time}
        type UserTable struct {*ibis.CF}
        func (t *UserTable) CF() *ibis.CF {
            t.CF = ibis.ReflectCF(User{})
            return t.SetPrimaryKey("Name")
        }
        type Model struct{Users *UserTable}
        model := &Model{}
        schema := ibis.ReflectSchema(model)

Using this reflective approach has the added benefit of eliminating the need to provide marshalling
code to get data into and out of User values, as will be described shortly.

Note that you must specify the primary key for the column family. This can be done with the
SetPrimaryKey method, but alternatively you can declare keys in the definition of reflected struct:

        type User struct {
            Name string `ibis:"key"`
            Password string
            CreatedAt time.Time
        }

Connecting to Cassandra

The DialCassandra method on Schema uses gocql to connect to and interact with a live Cassandra
cluster:

        schema.DialCassandra(CassandraConfig{Keyspace: "app", Node: []string{"localhost"}})

Alternatively, an in-memory fake Cassandra cluster can be used for testing:

        schema.Cluster = FakeCassandra()

Creating and Updating a Live Schema

A schema definition can also be read from the Cassandra cluster and compared to another definition.
A difference can be generated, along with CQL statements to make the live schema compatible with the
given one.

        schema := ...
        schema.DialCassandra(...)
        if schema.RequiresUpdates() {
            fmt.Println("Applying schema update:")
            fmt.Println(schema.SchemaUpdates)
            if err := schema.ApplyLiveUpdates(); err != nil {
                fmt.Println("error:", err)
            }
        }

Marshalling

The basis by which ibis reads and writes rows is the MarshalledMap. This is gocql marshalling
applied to a map of values. To use ibis at the lowest level, you can implement the Row interface:

        type User struct {Name string, Password string}
        func (u *User) Marshal(mmap ibis.MarshalledMap) error {
            mmap["Name"] = ibis.LiteralValue(u.Name)
            mmap["Password"] = ibis.LiteralValue(encryptPassword(u.Password))
        }
        func (u *User) Unmarshal(mmap ibis.MarshalledMap) error {
            var b []byte
            if v := mmap["Name"]; v != nil {
                if err := v.UnmarshalCQL(ibis.TIVarchar, &b); err != nil { return err }
                u.Name = string(b)
            }
            if v := mmap["Password"]; v != nil {
                if err := v.UnmarshalCQL(ibis.TIVarchar, &b); err != nil { return err }
                u.Password = decryptPassword(string(b))
            }
            return nil
        }

This is a bit tedious, of course, so ibis generates a Row implementation for you when you use
ReflectCF.

Interacting with Data

Once a CF is added to a schema that has been connected to a cluster, you can call methods
to fetch and store values implementing the Row interface. Here is an example of storing data using
reflection:

        type User struct {Name string `ibis:"key"`, Password string}
        type UserTable struct {*ibis.CF}
        func (t *UserTable) CF() *ibis.CF {
            t.CF = ibis.ReflectCF(User{})
            return t.CF
        }
        type Model struct{Users *UserTable}
        model := &Model{}
        schema := ibis.ReflectSchema(model)
        schema.Cluster = ibis.FakeCassandra()

        user := &User{Name: "logan", Password: "password"}
        if err := model.Users.CommitCAS(&user); err != nil { panic(err) }

        // oops, better change that password to something more secure
        user.Password = "hunter2"
        if err := model.Users.Commit(&user); err != nli { panic(err) }

Here is an example of fetching data:

        func Login(model *Model, name, password string) (user *User, err error) {
            if err = model.Users.LoadByKey(&user, name); err != nil { return nil, err }
            if user.password != password { return nil, errors.New("invalid password") }
            return user, nil
        }

Working with CQL

Sometimes you need to issue your own CQL statements to operate on your schema. ibis provides an
abstraction layer on top of gocql, but the interface is very similar. The main difference is that
a Query on a Cluster takes CQL values, rather than a string plus parameters. This is because ibis
heavily relies on building CQL statements in a declarative fashion.

        qi := ibis.InsertInto(model.Users).
                Keys("Name", "Password").
                Values(username, password).
                IfNotExists().Query()

        qi := ibis.Select("Title", "Url").From(model.Posts).
            Where("AuthorName = ?", username).
            Where("Published = true").
            OrderBy("PublishedAt DESC").
            Limit(10).Query()

        var b CQLBuilder
        b.Append("UPDATE users SET Password = ? WHERE Name = ?", "hunter2", "logan")
        qi := cluster.Query(b.CQL())

In each of these examples, qi is a Query pointer that provides access to results and errors.
*/
package ibis
