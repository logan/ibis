package main

import "fmt"
import "os"

import "github.com/logan/ibis"

type User struct {
	Name string `ibis:"key"`
}

type UserTable struct{ *ibis.CF }

func (t *UserTable) NewCF() (*ibis.CF, error) {
	var err error
	t.CF, err = ibis.ReflectCF(User{})
	return t.CF, err
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

func (t *PostTable) NewCF() (*ibis.CF, error) {
	var err error
	t.CF, err = ibis.ReflectCF(Post{})
	return t.CF, err
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
	schema, err := ibis.ReflectSchema(model)
	if err != nil {
		return nil, err
	}
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

	model, err := NewModel(ibis.FakeCassandra("test"))
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
