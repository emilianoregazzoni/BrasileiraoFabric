CREATE TABLE [dbo].[dimBrasileirao] (

	[year] bigint NULL, 
	[position] bigint NULL, 
	[teamKey] int NULL, 
	[teamName] varchar(8000) NULL, 
	[points] bigint NULL, 
	[games] bigint NULL, 
	[victories] bigint NULL, 
	[draws] bigint NULL, 
	[losses] bigint NULL, 
	[goals_scored] bigint NULL, 
	[goals_against] bigint NULL, 
	[goals_difference] bigint NULL
);