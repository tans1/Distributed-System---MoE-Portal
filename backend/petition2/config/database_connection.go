package config

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlDSN = "root:Emebet@1994@tcp(127.0.0.1:3306)/results?charset=utf8mb4&parseTime=True&loc=Local"
	serverAddress = "localhost:8080"
)

var Db *sql.DB

func init() {
	var err error
	Db, err = sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatal(err)
	}

	_, err = Db.Exec(`
	CREATE TABLE IF NOT EXISTS Petition (
		PetitionId INT AUTO_INCREMENT,
		Name varchar(256) Not Null,
		text TEXT,
		OwnerId int,
		PRIMARY KEY (PetitionId, Name),
		INDEX PetitionName (Name)
	);
	
	`)

	if err != nil {
		log.Println(err)
	}

	_, _ = Db.Exec(`
	CREATE TABLE IF NOT EXISTS Users (
	UserID INT PRIMARY KEY AUTO_INCREMENT,
	FirstName TEXT,
	LastName  TEXT,
	Email TEXT,
	Password TEXT
);

`)

if err != nil {
	log.Println(err)
}
	_, err = Db.Exec(`
	CREATE TABLE IF NOT EXISTS SignPetition (
		PetitionName varchar(256),
		UserId  BIGINT UNSIGNED,
		PRIMARY KEY (PetitionName, UserId),
		FOREIGN KEY (PetitionName) REFERENCES Petition(Name),
		FOREIGN KEY (userId) REFERENCES Users(id)
	);
	
	`)

	if err != nil {
		log.Println(err)
	}
}