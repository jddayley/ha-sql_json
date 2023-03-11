#sess = None
sess: Session | None = None
try:
    engine =  sqlalchemy.create_engine(db_url, future=True)
    sessmaker = scoped_session(sessionmaker(bind=engine, future=True))

    # Run a dummy query just to test the db_url
    sess = sessmaker()
    sess.execute(sqlalchemy.text("SELECT 1;"))

except sqlalchemy.exc.SQLAlchemyError as err:
    _LOGGER.error(
        "Couldn't connect using %s DB_URL: %s",
        redact_credentials(db_url),
        redact_credentials(str(err)),
    )
    return
finally:
    if sess:
        sess.close()

queries = []

for query in config.get(CONF_QUERIES):
    name = query.get(CONF_NAME)
    query_str = query.get(CONF_QUERY)
    unit = query.get(CONF_UNIT_OF_MEASUREMENT)
    value_template = query.get(CONF_VALUE_TEMPLATE)
    column_name = query.get(CONF_COLUMN_NAME)

    if value_template is not None:
        value_template.hass = hass

    # MSSQL uses TOP and not LIMIT
    if not ("LIMIT" in query_str or "SELECT TOP" in query_str):
        query_str = (
            query_str.replace("SELECT", "SELECT TOP 1")
            if "mssql" in db_url
            else query_str.replace(";", " LIMIT 1;")
        )

    sensor = SQLSensor(
        name, sessmaker, query_str, column_name, unit, value_template
    )
    queries.append(sensor)

add_entities(queries, True)
class SQLSensor(SensorEntity):
"""Representation of an SQL sensor."""

def __init__(self, name, sessmaker, query, column, unit, value_template):
    """Initialize the SQL sensor."""
    self._name = name
    self._query = query
    self._query_template = None
    if is_template_string(query):
        _LOGGER.debug("using template: %s", self._query)
        self._query_template = Template(query)
    self._unit_of_measurement = unit
    self._template = value_template
    self._column_name = column
    self.sessionmaker = sessmaker
    self._state = None
    self._attributes = None

@property
def name(self):
    """Return the name of the query."""
    return self._name

@property
def native_value(self):
    """Return the query's current state."""
    return self._state

@property
def native_unit_of_measurement(self):
    """Return the unit of measurement."""
    return self._unit_of_measurement

@property
def extra_state_attributes(self):
    """Return the state attributes."""
    return self._attributes

def update(self):
    """Retrieve sensor data from the query."""

    data = None
    try:
        sess = self.sessionmaker()
        if self._query_template:
            self._query_template.hass = self.hass
            self._query = self._query_template.render()
            _LOGGER.debug("query = %s", self._query)
        #result = sess.execute(self._query)
        result: Result = sess.execute(sqlalchemy.text(self._query))
        self._attributes = {}

        if not result.returns_rows or result.rowcount == 0:
            _LOGGER.warning("%s returned no results", self._query)
            self._state = None
            return

        for res in result.mappings():
            _LOGGER.debug("result = %s", res.items())
            data = res[self._column_name]
            for key, value in res.items():
                if isinstance(value, decimal.Decimal):
                    value = float(value)
                if isinstance(value, datetime.date):
                    value = str(value)
                try:
                    value_json = json.loads(value)
                    if isinstance(value_json, dict) or isinstance(value_json, list):
                        value = value_json
                except (ValueError, TypeError):
                    pass
                self._attributes[key] = value
    except sqlalchemy.exc.SQLAlchemyError as err:
        _LOGGER.error(
            "Error executing query %s: %s",
            self._query,
            redact_credentials(str(err)),
        )
        return
    finally:
        sess.close()

    if data is not None and self._template is not None:
        self._state = self._template.async_render_with_possible_json_value(
            data, None
        )
    else:
        self._state = data
