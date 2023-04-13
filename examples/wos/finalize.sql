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