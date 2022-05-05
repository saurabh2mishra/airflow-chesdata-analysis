CREATE TABLE IF NOT EXISTS country_lookup_table AS
        SELECT * FROM country_abbr
        INNER JOIN chesdata ON
        country_abbr.CountryID = chesdata.country
        INNER JOIN party ON
        party.party_id = chesdata.party_id;



