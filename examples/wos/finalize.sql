-- Duplicate removal --

-- @xplinter Removing duplicates from publisher
DROP TABLE IF EXISTS _publisher;
CREATE TABLE _publisher AS
    SELECT DISTINCT ON (id) *
    FROM publisher;
DROP TABLE publisher;
ALTER TABLE _publisher RENAME TO publisher;

-- @xplinter Removing duplicates from source
DROP TABLE IF EXISTS _source;
CREATE TABLE _source AS
    SELECT DISTINCT ON (id) *
    FROM source;
DROP TABLE source;
ALTER TABLE _source RENAME TO source;

-- @xplinter Removing duplicates from descriptor
DROP TABLE IF EXISTS _descriptor;
CREATE TABLE _descriptor AS
    SELECT * FROM (
        SELECT DISTINCT ON (id) *
        FROM descriptor
    ) AS t
    ORDER BY type, text;
DROP TABLE descriptor;
ALTER TABLE _descriptor RENAME TO descriptor;

-- @xplinter Removing duplicates from address
DROP TABLE IF EXISTS _address;
CREATE TABLE _address AS
    SELECT DISTINCT ON (id) *
    FROM address;
DROP TABLE address;
ALTER TABLE _address RENAME TO address;

-- @xplinter Removing duplicates from conference
DROP TABLE IF EXISTS _conference;
CREATE TABLE _conference AS
    SELECT DISTINCT ON (id) *
    FROM conference;
DROP TABLE conference;
ALTER TABLE _conference RENAME TO conference;

-- @xplinter Removing duplicates from conference_sponsor
DROP TABLE IF EXISTS _conference_sponsor;
CREATE TABLE _conference_sponsor AS
    SELECT DISTINCT *
    FROM conference_sponsor;
DROP TABLE conference_sponsor;
ALTER TABLE _conference_sponsor RENAME TO conference_sponsor;

-- Grants --

-- @xplinter Dealing with grants

DROP TABLE IF EXISTS _agency;
DROP TABLE IF EXISTS _grant_agency;
DROP TABLE IF EXISTS _grant_pi;
DROP TABLE IF EXISTS _publication_grant;
DROP TABLE IF EXISTS _grant_data;

CREATE TABLE _agency AS
    SELECT * FROM (
        SELECT DISTINCT ON (id) *
        FROM agency
    ) AS t
    ORDER BY name;

CREATE TABLE _grant_agency AS
    SELECT DISTINCT *
    FROM grant_agency;

CREATE TABLE _grant_pi AS
    SELECT DISTINCT *
    FROM grant_pi
    WHERE name IS NOT NULL;

CREATE TABLE _grant_data AS
    SELECT DISTINCT *
    FROM grant_data;

CREATE TABLE _publication_grant AS
    SELECT DISTINCT *
    FROM publication_grant;

DROP TABLE agency;
DROP TABLE grant_agency;
DROP TABLE grant_pi;
DROP TABLE publication_grant;
DROP TABLE grant_data;

ALTER TABLE _agency            RENAME TO agency;
ALTER TABLE _grant_agency      RENAME TO grant_agency;
ALTER TABLE _grant_pi          RENAME TO grant_pi;
ALTER TABLE _publication_grant RENAME TO publication_grant;
ALTER TABLE _grant_data        RENAME TO grant_data;