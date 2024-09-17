package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	config "petition2/config"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	docMutex sync.Mutex
)
var cache = make(map[string]TextDocument)

type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	Send     chan *TextDocument
	Document TextDocument
}

type User struct {
	FirstName string
	LastName  string
	Email     string
}

type TextDocument struct {
	Title        string
	Text         string
	OwnerId      int
}

type SignPetition struct {
	UserId     int
	PetitionName string
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan *TextDocument
	register   chan *Client
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *TextDocument),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
			}
		case doc := <-h.broadcast:
			cache[doc.Title] = *doc
			for client := range h.clients {
				client.Document.Text = string(doc.Text)
				select {
				case client.Send <- &client.Document:
				default:
					close(client.Send)
					delete(h.clients, client)
				}
			}
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func saveDocument(document TextDocument) (TextDocument,error) {


	if document.Title == "" {
		return document,errors.New("title must not be empty")
	}

	query := `INSERT INTO Petition(Name, text, OwnerId)
	 VALUES (?, ?, ?)`
	_, err := config.Db.Exec(query, document.Title, document.Text, document.OwnerId)

	return document,err
}

func handleConnections(hub *Hub, c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	
	var doc TextDocument
	documentName := c.Request.URL.Query().Get("document")
	if document, ok := cache[documentName]; ok {
		doc = document
	} else {
		doc,_ = getDocument(documentName)
		cache[documentName] = doc
	}
	
	client := &Client{hub: hub, conn: conn, Send: make(chan *TextDocument), Document: doc}
	hub.register <- client

	go client.write()
	go client.read()
	client.Send <- &doc
}

func getDocument(documentName string) (TextDocument,error) {

	var (
		Name         string
		text         string
		OwnerId      int
	)

	err := config.Db.QueryRow(`SELECT Name,text,OwnerId 
	FROM Petition where Name = ? ORDER BY PetitionId DESC LIMIT 1`, documentName).Scan(&Name, &text, &OwnerId)
	docMutex.Lock()
	doc := TextDocument{
		Title:        Name,
		Text:         text,
		OwnerId:      OwnerId,
	}

	docMutex.Unlock()

	return doc,err

}


func (c *Client) write() {
	defer c.conn.Close()
	for{
		select {
		case doc, ok := <-c.Send:
			if !ok {
				return
			}
			c.Document.Text = doc.Text
			c.conn.WriteMessage(websocket.TextMessage, []byte(doc.Text))
		}
	}
}
func (c *Client) read() {
	defer func() {

		if len(c.hub.clients) == 1 {
			saveDocument(cache[c.Document.Title])
		}
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		c.Document.Text = string(message)
		if err != nil {
			break
		}

		c.hub.broadcast <- &c.Document
	}
}


func getAll()( []TextDocument,error) {

	query := `SELECT Name, Text, OwnerId
	FROM Petition
	WHERE (PetitionId, Name) IN 
		(SELECT MAX(PetitionId), Name
		 FROM Petition
		 GROUP BY Name);
	`
	rows, err := config.Db.Query(query)
	
	defer rows.Close()

	var res []TextDocument = make([]TextDocument, 0)
	for rows.Next() {
		var name string
		var OwnerId int
		var Text string
		if err := rows.Scan(&name, &Text, &OwnerId); err != nil {
			continue
		}

		res = append(res, TextDocument{Title: name, OwnerId: OwnerId, Text: Text})
	}

	return res,err

}

func getAllPetitions(c *gin.Context) {
	petitions,err := getAll()
	if (err != nil){
		c.IndentedJSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve petitions"})
		return
	}else{

		c.IndentedJSON(http.StatusOK, petitions)
	}
}

func signPetition(c *gin.Context) {
	var signPetition SignPetition
	if err := c.BindJSON(&signPetition); err != nil {
		return
	}

	query := "INSERT INTO SignPetition(PetitionName,UserId) VALUES (?, ?)"
	_, err := config.Db.Exec(query, signPetition.PetitionName, signPetition.UserId)

	if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to sign petition"})
        return
    }

    c.JSON(http.StatusOK, gin.H{"message": "Petition signed successfully", "signPetition": signPetition})
}

func createPetition(c *gin.Context) {
	var document TextDocument
	if err := c.BindJSON(&document); err != nil {
		return
	}
	_,err := getDocument(document.Title)
	if (err == nil){
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create petition"})
		return
	}

	doc,err := saveDocument(document)

	if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create petition"})
        return
    }
    c.JSON(http.StatusOK, gin.H{"message": "Petition created successfully", "petition": doc})

}



func getSignatories(c *gin.Context) {
	petitionName := c.Query("PetitionName")
	query := `SELECT first_name, last_name, email FROM Users JOIN SignPetition ON Users.id = SignPetition.UserId WHERE SignPetition.PetitionName = ` + petitionName + " "
	rows, err := config.Db.Query(query)
	if (err != nil){
		fmt.Println(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve signatories"})
		return 
	}

	defer rows.Close()
	var users = make([]User, 0)
	for rows.Next() {
		var user = User{}
		err := rows.Scan(&user.FirstName, &user.LastName, &user.Email)
		if err != nil {
			log.Print(err)
		}
		users = append(users, user)

	}
	c.IndentedJSON(http.StatusOK, users)
}

func save(cache map[string]TextDocument) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            saveCache(cache)
        }
    }
}

func saveCache(cache map[string]TextDocument) {
    for title, value := range cache {
        
        _,err := saveDocument(value)
        if err != nil {
            fmt.Printf("Error saving document %s: %v\n", title, err)
        }
    }
}


func main() {
	hub := newHub()
	go hub.run()
	go save(cache)
	router := gin.Default()

	router.GET("/ws", func(c *gin.Context) {
		handleConnections(hub, c)
	})
	router.GET("/petitions", getAllPetitions)
	router.POST("/createPetition", createPetition)
	router.POST("/signPetition", signPetition)
	router.GET("/signatories", getSignatories)
	router.Run("localhost:3032")
	log.Println("Server is running on :3032")

}
