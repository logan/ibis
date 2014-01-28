/*
Package timeline stores and retrieves timeseries-like data.

Timelines

A timeline is a uniquely named series of timestamped blobs corresponding to events or objects. With
proper management, each timeline can be efficiently updated and scanned regardless of event
frequency.

Typically each associated blob is either an encoded foreign key or materialization of an entire
object being indexed. The timestamp corresponds to some event in the object's lifetime. Entries may
be removed from a timeline and readded, provided the former and new timestamps are known.

Example

        index := schema.IndexTable.Index("posts")
        batch := make([]ibis.CQL, 0)
        for _, post := range posts {
            cluster.Query(index.Add(post.PublishedAtSeqID, post.ToJson()))).Exec()
        }

        myPosts := make([]*Post, 0)
        index = schema.IndexTable.Index("postsBy", username)
        for entry := range index.Scanner().Start() {
            post := new(Post)
            post.FromJson(entry.Blob)
        }
*/
package timeline
