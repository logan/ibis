package main

import "fmt"
import "os"

import "github.com/logan/ibis"

type User struct {
	Name string
}

type UserTable struct {
	*ibis.ColumnFamily
}

func (t *UserTable) CF() *ibis.ColumnFamily {
	t.ColumnFamily = ibis.ReflectColumnFamily(User{})
	return t.Key("Name")
}

type Post struct {
	ibis.SeqID
	AuthorName string
	Title      string
	Body       string
}

type PostTable struct {
	*ibis.ColumnFamily
}

func (t *PostTable) CF() *ibis.ColumnFamily {
	t.ColumnFamily = ibis.ReflectColumnFamily(Post{})
	return t.Key("SeqID")
}

type Model struct {
	ibis.SeqIDGenerator
	Users *UserTable
	Posts *PostTable
}

func NewModel(cluster ibis.Cluster) (*Model, error) {
	idgen, err := ibis.NewSeqIDGenerator()
	if err != nil {
		return nil, err
	}
	model := &Model{SeqIDGenerator: idgen}
	schema := ibis.ReflectSchemaFrom(model)
	schema.Cluster = cluster

	if schema.SchemaUpdates, err = ibis.DiffLiveSchema(cluster, schema); err != nil {
		return nil, err
	}
	if err = schema.ApplySchemaUpdates(); err != nil {
		return nil, err
	}
	return model, nil
}

func (m *Model) NewUser(name string) (*User, error) {
	user := &User{Name: name}
	if err := m.Users.CommitCAS(user); err != nil {
		return nil, err
	}
	return user, nil
}

func (m *Model) NewPost(author *User, title, body string) (*Post, error) {
	post := &Post{AuthorName: author.Name, Title: title, Body: body}
	if err := m.Posts.CommitCAS(post); err != nil {
		return nil, err
	}
	return post, nil
}

func main() {
	fail := func(err error) {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	model, err := NewModel(ibis.FakeCassandra())
	if err != nil {
		fail(err)
	}
	user, err := model.NewUser("logan")
	if err != nil {
		fail(err)
	}
	post, err := model.NewPost(user, "test post", "please ignore")
	if err != nil {
		fail(err)
	}
	fmt.Printf("Post:\n  %+v\n", *post)
}
