DROP TABLE IF EXISTS Actors,Actor_Junction,Genres,Genre_Junction,list,list_2,list_3,countries,country_junction;
-- drop table netflix_showsz

SELECT DISTINCT actor_0 AS "Actor"
INTO list
FROM netflix_shows

SELECT * FROM list
INSERT INTO list (Actor)
SELECT DISTINCT actor_1 FROM netflix_shows WHERE actor_1 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_2 FROM netflix_shows WHERE actor_2 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_3 FROM netflix_shows WHERE actor_3 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_4 FROM netflix_shows WHERE actor_4 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_5 FROM netflix_shows WHERE actor_5 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_6 FROM netflix_shows WHERE actor_6 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_7 FROM netflix_shows WHERE actor_7 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_8 FROM netflix_shows WHERE actor_8 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_9 FROM netflix_shows WHERE actor_9 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_10 FROM netflix_shows WHERE actor_10 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_11 FROM netflix_shows WHERE actor_11 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_12 FROM netflix_shows WHERE actor_12 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_13 FROM netflix_shows WHERE actor_13 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_14 FROM netflix_shows WHERE actor_14 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_15 FROM netflix_shows WHERE actor_15 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_16 FROM netflix_shows WHERE actor_16 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_17 FROM netflix_shows WHERE actor_17 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_18 FROM netflix_shows WHERE actor_18 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_19 FROM netflix_shows WHERE actor_19 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_20 FROM netflix_shows WHERE actor_20 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_21 FROM netflix_shows WHERE actor_21 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_22 FROM netflix_shows WHERE actor_22 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_23 FROM netflix_shows WHERE actor_23 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_24 FROM netflix_shows WHERE actor_24 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_25 FROM netflix_shows WHERE actor_25 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_26 FROM netflix_shows WHERE actor_26 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_27 FROM netflix_shows WHERE actor_27 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_28 FROM netflix_shows WHERE actor_28 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_29 FROM netflix_shows WHERE actor_29 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_30 FROM netflix_shows WHERE actor_30 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_31 FROM netflix_shows WHERE actor_31 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_32 FROM netflix_shows WHERE actor_32 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_33 FROM netflix_shows WHERE actor_33 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_34 FROM netflix_shows WHERE actor_34 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_35 FROM netflix_shows WHERE actor_35 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_36 FROM netflix_shows WHERE actor_36 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_37 FROM netflix_shows WHERE actor_37 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_38 FROM netflix_shows WHERE actor_38 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_39 FROM netflix_shows WHERE actor_39 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_40 FROM netflix_shows WHERE actor_40 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_41 FROM netflix_shows WHERE actor_41 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_42 FROM netflix_shows WHERE actor_42 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_43 FROM netflix_shows WHERE actor_43 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_44 FROM netflix_shows WHERE actor_44 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_45 FROM netflix_shows WHERE actor_45 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_46 FROM netflix_shows WHERE actor_46 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_47 FROM netflix_shows WHERE actor_47 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_48 FROM netflix_shows WHERE actor_48 IS NOT NULL
UNION ALL 
SELECT DISTINCT actor_49 FROM netflix_shows WHERE actor_49 IS NOT NULL

SELECT DISTINCT Actor 
INTO Actors
FROM list
ORDER BY Actor DESC

SELECT DISTINCT genre_0 AS "Genre"
INTO list_2
FROM netflix_shows

SELECT * FROM list_2
INSERT INTO list_2 (Genre)
SELECT DISTINCT genre_1 FROM netflix_shows WHERE genre_1 IS NOT NULL
UNION ALL 
SELECT DISTINCT genre_2 FROM netflix_shows WHERE genre_2 IS NOT NULL

SELECT DISTINCT Genre 
INTO Genres
FROM list_2
ORDER BY Genre

SELECT DISTINCT country_0 AS "Country"
INTO list_3
FROM netflix_shows

SELECT * FROM list_3
INSERT INTO list_3 (Country)
SELECT DISTINCT country_1 FROM netflix_shows WHERE country_1 IS NOT NULL
UNION ALL 
SELECT DISTINCT country_2 FROM netflix_shows WHERE country_2 IS NOT NULL
UNION ALL 
SELECT DISTINCT country_3 FROM netflix_shows WHERE country_3 IS NOT NULL
UNION ALL 
SELECT DISTINCT country_4 FROM netflix_shows WHERE country_4 IS NOT NULL
UNION ALL 
SELECT DISTINCT country_5 FROM netflix_shows WHERE country_5 IS NOT NULL
UNION ALL 
SELECT DISTINCT country_6 FROM netflix_shows WHERE country_6 IS NOT NULL
UNION ALL 
SELECT DISTINCT country_7 FROM netflix_shows WHERE country_7 IS NOT NULL


SELECT DISTINCT Country
INTO Countries
FROM list_3


ALTER TABLE Actors ADD ActorID INT PRIMARY KEY IDENTITY(1,1)

ALTER TABLE Genres ADD GenreID INT PRIMARY KEY IDENTITY(1,1)

ALTER TABLE Countries ADD CountryID INT PRIMARY KEY IDENTITY(1,1)

ALTER TABLE netflix_shows ADD showID INT PRIMARY KEY IDENTITY(1,1)
GO

SELECT showID, genre
INTO Genre_Junction
FROM (
    SELECT showID, genre_0, genre_1, genre_2
    FROM netflix_shows
) AS SourceTable
UNPIVOT (
    genre FOR GenreColumns IN (genre_0, genre_1, genre_2)
) AS UnpivotedTable
WHERE genre IS NOT NULL;

SELECT showID, country
INTO Country_Junction
FROM (
    SELECT showID, country_0, country_1, country_2, country_3, country_4, country_5, country_6, country_7
    FROM netflix_shows
) AS SourceTable
UNPIVOT (
    country FOR CountryColumns IN (country_0, country_1, country_2, country_3, country_4, country_5, country_6, country_7)
) AS UnpivotedTable
WHERE country IS NOT NULL;

SELECT showID, actor
INTO Actor_Junction
FROM (
    SELECT showID, [actor_0]
      ,[actor_1]
      ,[actor_2]
      ,[actor_3]
      ,[actor_4]
      ,[actor_5]
      ,[actor_6]
      ,[actor_7]
      ,[actor_8]
      ,[actor_9]
      ,[actor_10]
      ,[actor_11]
      ,[actor_12]
      ,[actor_13]
      ,[actor_14]
      ,[actor_15]
      ,[actor_16]
      ,[actor_17]
      ,[actor_18]
      ,[actor_19]
      ,[actor_20]
      ,[actor_21]
      ,[actor_22]
      ,[actor_23]
      ,[actor_24]
      ,[actor_25]
      ,[actor_26]
      ,[actor_27]
      ,[actor_28]
      ,[actor_29]
      ,[actor_30]
      ,[actor_31]
      ,[actor_32]
      ,[actor_33]
      ,[actor_34]
      ,[actor_35]
      ,[actor_36]
      ,[actor_37]
      ,[actor_38]
      ,[actor_39]
      ,[actor_40]
      ,[actor_41]
      ,[actor_42]
      ,[actor_43]
      ,[actor_44]
      ,[actor_45]
      ,[actor_46]
      ,[actor_47]
      ,[actor_48]
      ,[actor_49]
    FROM netflix_shows
) AS SourceTable
UNPIVOT (
    actor FOR ActorColumns IN ([actor_0]
      ,[actor_1]
      ,[actor_2]
      ,[actor_3]
      ,[actor_4]
      ,[actor_5]
      ,[actor_6]
      ,[actor_7]
      ,[actor_8]
      ,[actor_9]
      ,[actor_10]
      ,[actor_11]
      ,[actor_12]
      ,[actor_13]
      ,[actor_14]
      ,[actor_15]
      ,[actor_16]
      ,[actor_17]
      ,[actor_18]
      ,[actor_19]
      ,[actor_20]
      ,[actor_21]
      ,[actor_22]
      ,[actor_23]
      ,[actor_24]
      ,[actor_25]
      ,[actor_26]
      ,[actor_27]
      ,[actor_28]
      ,[actor_29]
      ,[actor_30]
      ,[actor_31]
      ,[actor_32]
      ,[actor_33]
      ,[actor_34]
      ,[actor_35]
      ,[actor_36]
      ,[actor_37]
      ,[actor_38]
      ,[actor_39]
      ,[actor_40]
      ,[actor_41]
      ,[actor_42]
      ,[actor_43]
      ,[actor_44]
      ,[actor_45]
      ,[actor_46]
      ,[actor_47]
      ,[actor_48]
      ,[actor_49])
) AS UnpivotedTable
WHERE actor IS NOT NULL;

UPDATE Genre_Junction
SET genre = (SELECT GenreID FROM Genres WHERE Genre_Junction.genre = Genres.genre )

UPDATE Actor_Junction
SET actor = (SELECT ActorID FROM Actors WHERE Actor_Junction.actor = Actors.actor )

UPDATE Country_Junction
SET country = (SELECT CountryID FROM Countries WHERE Country_Junction.country = Countries.country )


EXEC sp_rename 'Actor_Junction.actor', 'ActorID';
EXEC sp_rename 'Genre_Junction.genre', 'GenreID';
EXEC sp_rename 'Country_Junction.country', 'CountryID';


ALTER TABLE Actor_Junction
ALTER COLUMN ActorID INT 


ALTER TABLE Actor_Junction
ADD CONSTRAINT ActorID FOREIGN KEY (ActorID) REFERENCES Actors(ActorID)

ALTER TABLE Genre_Junction
ALTER COLUMN GenreID INT 

ALTER TABLE Genre_Junction
ADD CONSTRAINT GenreID FOREIGN KEY (GenreID) REFERENCES Genres(GenreID)

ALTER TABLE Country_Junction
ALTER COLUMN CountryID INT 

ALTER TABLE Country_Junction
ADD CONSTRAINT CountryID FOREIGN KEY (CountryID) REFERENCES Countries(COuntryID)

DROP TABLE list 
DROP TABLE list_2
DROP TABLE list_3


ALTER TABLE netflix_shows
DROP COLUMN [actor_0]
      ,[actor_1]
      ,[actor_2]
      ,[actor_3]
      ,[actor_4]
      ,[actor_5]
      ,[actor_6]
      ,[actor_7]
      ,[actor_8]
      ,[actor_9]
      ,[actor_10]
      ,[actor_11]
      ,[actor_12]
      ,[actor_13]
      ,[actor_14]
      ,[actor_15]
      ,[actor_16]
      ,[actor_17]
      ,[actor_18]
      ,[actor_19]
      ,[actor_20]
      ,[actor_21]
      ,[actor_22]
      ,[actor_23]
      ,[actor_24]
      ,[actor_25]
      ,[actor_26]
      ,[actor_27]
      ,[actor_28]
      ,[actor_29]
      ,[actor_30]
      ,[actor_31]
      ,[actor_32]
      ,[actor_33]
      ,[actor_34]
      ,[actor_35]
      ,[actor_36]
      ,[actor_37]
      ,[actor_38]
      ,[actor_39]
      ,[actor_40]
      ,[actor_41]
      ,[actor_42]
      ,[actor_43]
      ,[actor_44]
      ,[actor_45]
      ,[actor_46]
      ,[actor_47]
      ,[actor_48]
      ,[actor_49]
      ,genre_0
      ,genre_1
      ,genre_2
      ,country_0
      ,country_1
      ,country_2
      ,country_3
      ,country_4
      ,country_5
      ,country_6
      ,country_7
      ,show_id

ALTER TABLE netflix_shows
ALTER COLUMN date_added DATE