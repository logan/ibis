package main

import "fmt"
import "os"

import "github.com/logan/ibis"

type User struct {
	Name string `ibis:"key"`
}

type UserTable struct{ *ibis.CF }

func (t *UserTable) NewCF() *ibis.CF {
	t.CF = ibis.ReflectCF(User{})
	return t.CF
}

type Post struct {
	ibis.SeqID `ibis:"key"`
	AuthorName string
	Title      string
	Body       string
}

type PostTable struct {
	*ibis.CF
}

func (t *PostTable) NewCF() *ibis.CF {
	t.CF = ibis.ReflectCF(Post{})
	return t.CF
}

type Model struct {
	Users *UserTable
	Posts *PostTable
}

func NewModel(cluster ibis.Cluster) (*Model, error) {
	idgen, err := ibis.NewSeqIDGenerator()
	if err != nil {
		return nil, err
	}

	model := &Model{}
	schema := ibis.ReflectSchema(model)
	schema.Provide(idgen)
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
