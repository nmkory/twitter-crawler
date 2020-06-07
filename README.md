# CS 172 Project

A search engine for Twitter.

## How to use our LuceneBuilder class

Import the LuceneBuilder class and create a new LuceneBuilder object. Choose one of the following constructors.

```java
/*
 * Create a new LuceneBuilder object using default file structure. This assumes the index will be up
 * one level and in a file called "index" and the .json files are up one level and in a file called
 * "Crawler". This should be used if the file structure has not been moved.
 */
LuceneBuilder luceneIndex = new LuceneBuilder();

/*
 * Create a new LuceneBuilder object using parameterized file structure. This assumes the index will be
 * up one level and in a file called "index" and the .json files are up one level and in a file called
 * "Crawler". This should be used if the file structure is moved around.
 */
LuceneBuilder luceneIndex = new LuceneBuilder("../index", "../Crawler");
```

After creating the LuceneBuilder object, you may, if you want, build the index. This should only be done if there are no .json files currently in the index or if there are new .json files to add to the existing index. This should not be called if the index already exists and there are only "already added" files to add (doing so creates duplicate entries in the index). I've uploaded the index to GitHub;  there should be no need to add or recreate it but you can to play around with it if you want.

```java
/*
 * Creates a new index or amends an existing index. Do not use if the index already exists (as it does
 * on GitHub).
 */
luceneIndex.buildIndex();
```

After confirming that that the index exists, you need to explicitly call buildSearcher(). Doing so sets up the index for searching.

```java
luceneIndex.buildSearcher();
```

The index is now ready to be searched. To get search results for the API, pass the front-end search term to the LuceneBuilder object method search(String searchTerm, int maxResults). This returns an object called TopDocs which you will use to pass back to the front-end using the REST API.

```java
/*
 * Below is an example of seaching the index for covid19 and getting back up to 100 results.
 */
TopDocs hits = luceneIndex.search("covid19", 100);
```

The variable hits now contains your search results. You can see how many results you have by calling:
```java
hits.scoreDocs.length;
```

You'll need to loop through hits to get the information necessary for the front-end. Here is how you should extrapolate information from variable hits:
```java
/*
 * This just loops through the variable hits for all the search term results. They are already in order.
 * Use the get() function as shown below to build the necessary objects for the front-end.
 */
for(ScoreDoc scoreDoc : hits.scoreDocs) {
            Document doc = luceneIndex.searcher.doc(scoreDoc.doc);
            String tweetText = doc.get("text");
            String tweetUser = ("@" + doc.get("user");
            String tweetDate = doc.get("datetime");
            String tweetLat = doc.get("latitude");
            String tweetLong = doc.get("longitude");
            String tweetURL = doc.get("url");
        }
```

That's it. If you want to be efficient, you can use the same LuceneBuilder object and seach it again using a new term. Alternatively you could create a new object every time search is run. It shouldn't matter as Java will do the garbage collection automatically.
