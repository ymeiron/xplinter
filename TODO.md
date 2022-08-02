TODO
====

* Maybe make entity parent an optional argument with the default being the root entity. For no-parent entities, use "NULL" as parent.

* Sometimes we want to parse a single element but have the Python function retun multiple fields (like the month element gives us month and potentially day, the conf_state gives us admin_div, country and online). Maybe something along:

-

    ENTITY conference (ZERO OR MORE, *root, XPATH(static_data/summary/conferences/conference)) {
        *location    OPT   FIELDGROUP XPATH(conf_locations/conf_location/conf_state) @ PYTHON(parse_conf_location),
        wos_id             TEXT       PARENTFIELD(0),
        id           OPT   INTEGER    XPATH(@conf_id),
        title        OPT   TEXT       XPATH(conf_titles/conf_title),
        date_start   OPT   INTEGER    XPATH(conf_dates/conf_date/@conf_start) @ PYTHON(parse_date),
        date_end     OPT   INTEGER    XPATH(conf_dates/conf_date/@conf_end) @ PYTHON(parse_date),
        city         OPT   TEXT       XPATH(conf_city),
        admin_div    OPT   TEXT       FIELDGROUP(*location, 0)
        country      OPT   TEXT       FIELDGROUP(*location, 1)
        online       OPT   BOOLEAN    FIELDGROUP(*location, 2)
    }

With this syntax the resulting fields can be in a any order.