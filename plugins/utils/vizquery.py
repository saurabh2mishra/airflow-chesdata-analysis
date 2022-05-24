query_for_countrywise_agg_orientation = """
            SELECT Country
            ,cast(round({}) AS INT) AS country_orientation
            ,count(1) AS count_country_orientation
          FROM country_lookup_table
          where Country in {}
          GROUP BY Country
            ,country_orientation
            """
query_for_country_party_orientation = """
            select Country, 
            party,
            cast(round({}) as int) as country_orientation
            from country_lookup_table
            where Country in {}
          """
