#!.venv/bin/python3
# -*- coding: utf-8 -*-

import argparse
from homeassistant.core import Event, State
from homeassistant.components.influxdb import get_influx_connection, _generate_event_to_json, INFLUX_SCHEMA
from homeassistant.exceptions import InvalidEntityFormatError
import json
import sys
from tqdm import tqdm
import voluptuous as vol
import yaml
from datetime import datetime

sys.path.append("home-assistant-core")

# MySQL / MariaDB
try:
    from MySQLdb import connect as mysql_connect, cursors, Error
except:
    print("Warning: Could not load Mysql driver, might not be a problem if you intend to use sqlite")

# SQLite (not tested)
try:
    import sqlite3
except:
    print("Warning: Could not load sqlite3 driver, might not be a problem if you intend to use Mysql")


def rename_entity_id(old_name: str) -> str:
    """
    Given an entity_id, rename it to something else. Helpful if ids changed
    during the course of history and you want to quickly merge the data. Beware
    that no further adjustment is done, also no checks whether the referred
    sensors are even compatible.
    """
    rename_table = {
            "sensor.old_entity_name": "sensor.new_entity_name",
    }

    if old_name in rename_table:
        return rename_table[old_name]

    return old_name


def rename_friendly_name(attributes: dict) -> dict:
    """
    Given the attributes to be stored, replace the friendly name. Helpful
    if names changed during the course of history and you want to quickly
    correct the naming.
    """
    rename_table = {
            "Old Sensor Name": "New Sensor Name",
    }

    if "friendly_name" in attributes and attributes["friendly_name"] in rename_table:
        attributes["friendly_name"] = rename_table[attributes["friendly_name"]]

    return attributes


def create_statistics_attributes(mean: float, min: float, max: float, attributes: dict) -> dict:
    """
    Add min, max, mean to attributes. Run friendly name converter
    """
    attributes = rename_friendly_name(attributes)
    attributes['mean'] = mean
    attributes['min'] = min
    attributes['max'] = max
    return attributes


def main():
    """
    Connect to both databases and migrate data
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--type', '-t',
                        dest='type', action='store', required=True,
                        help='Database type: MySQL, MariaDB or SQLite')
    parser.add_argument('--user', '-u',
                        dest='user', action='store', required=False,
                        help='MySQL/MariaDB username')
    parser.add_argument('--password', "-p",
                        dest='password', action='store', required=False,
                        help='MySQL/MariaDB password')
    parser.add_argument('--host', '-s',
                        dest='host', action='store', required=False,
                        help='MySQL/MariaDB host')
    parser.add_argument('--database', '-d',
                        dest='database', action='store', required=True,
                        help='MySQL/MariaDB database or SQLite databasefile')
    parser.add_argument('--count', '-c',
                        dest='row_count', action='store', required=False, type=int, default=0,
                        help='If 0 (default), determine upper bound of number of rows by querying database, '
                             'otherwise use this number (used for progress bar only)')
    parser.add_argument('--table', '-x',
                        dest='table', action='store', required=False, default='both',
                        help='Source Table is either states or statistics or both'
                             'Home Assistant keeps 10 days of states by default and keeps statistics forever for some entities')
    parser.add_argument('--dry-run', '-y',
                        dest='dry', action='store_true', required=False,
                        help='do all work except writing to InfluxDB')

    args = parser.parse_args()

    if (args.dry):
        print("option --dry-run was given, nothing will be writen on InfluxDB")

    # load InfluxDB configuration file (the one from Home Assistant) (without using !secrets)
    with open("influxdb.yaml") as config_file:
        influx_config = yaml.load(config_file, Loader=yaml.FullLoader)

    # validate and extend config
    schema = vol.Schema(INFLUX_SCHEMA, extra=vol.ALLOW_EXTRA)
    influx_config = schema(influx_config)

    if influx_config.get('token') == "TOKEN_HERE":
        print('oopsie woopsie we made a fucky wucky, we need to add our influx credentials to influxdb.yaml')
        exit()

    # establish connection to InfluxDB
    influx = get_influx_connection(influx_config, test_write=True, test_read=True)
    converter = _generate_event_to_json(influx_config)

    if args.type == "MySQL" or args.type == "MariaDB":
        # connect to MySQL/MariaDB database
        connection = mysql_connect(host=args.host,
                                   user=args.user,
                                   password=args.password,
                                   database=args.database,
                                   cursorclass=cursors.SSCursor,
                                   charset="utf8")
    else:
        # connect to SQLite file instead
        connection = sqlite3.connect(args.database)

    # Create list tables that we are going to retrieve from HA
    tables = get_tables(args.table)
    print(f"Migrating home assistant database {', '.join(tables)} to Influx database {args.database} and " +
          f"bucket {influx_config.get('bucket')}")
    # write to influxdb in batches
    influx_batch_size_max = 1024
    influx_batch_size_cur = 0
    influx_batch_json = []

    if 'statistics' in tables:
        cursor = connection.cursor()
        tmp_table_query = formulate_tmp_table_sql()
        cursor.execute(tmp_table_query)
        cursor.close()

    # select the values we are interested in
    for table in tables:
        print(f"Running SQL query on database table {table}." +
              "This may take longer than a few minutes, depending on how many rows there are in the database.")
        if args.row_count == 0:
            # query number of rows in states table - this will be more than the number of rows we
            # are going to process, but at least it gives us some percentage and estimation
            cursor = connection.cursor()
            cursor.execute(f"select COUNT(*) from {table}")
            total = cursor.fetchone()[0]
            cursor.close()
        else:
            total = args.row_count

        # map to count names and number of measurements for each entity
        statistics = {}
        # Execute correct query for table
        sql_query = formulate_sql_query(table, args.table)
        cursor = connection.cursor()
        cursor.execute(sql_query)

        # Loop over each data row
        print(f"    Processing rows from table {table} and writing to InfluxDB.")
        with tqdm(total=total, mininterval=1, maxinterval=5, unit=" rows", unit_scale=True, leave=False) as progress_bar:
            try:
                row_counter = 0
                for row in cursor:
                    progress_bar.update(1)
                    row_counter += 1
                    try:
                        if table == "states":
                            _entity_id = rename_entity_id(row[0])
                            _state = row[1]
                            _attributes_raw = row[2]
                            _attributes = rename_friendly_name(json.loads(_attributes_raw))
                            _event_type = row[3]
                            _time_fired = row[4]
                        elif table == "statistics":
                            _entity_id = rename_entity_id(row[0])
                            _state = row[1]
                            _mean = row[1]
                            _min = row[2]
                            _max = row[3]
                            _attributes_raw = row[4]
                            _attributes = create_statistics_attributes(_mean, _min, _max, json.loads(_attributes_raw))
                            _event_type = row[5]
                            _time_fired = row[6]
                    except Exception as e:
                        print("Failed extracting data from %s: %s.\nAttributes: %s" % (row, e, _attributes_raw))
                        continue

                    try:
                        # recreate state and event
                        state = State(
                            entity_id=_entity_id,
                            state=_state,
                            attributes=_attributes)
                        if args.type == "MySQL" or args.type == "MariaDB":
                            event = Event(
                                _event_type,
                                data={"new_state": state},
                                time_fired=_time_fired
                            )
                        else:
                            event = Event(
                                _event_type,
                                data={"new_state": state},
                                time_fired=datetime.strptime(_time_fired, "%Y-%m-%d %H:%M:%S.%f")
                            )
                    except InvalidEntityFormatError:
                        pass
                    else:
                        data = converter(event)
                        if not data:
                            continue

                        # collect statistics (remove this code block to speed up processing slightly)
                        if "friendly_name" in _attributes:
                            friendly_name = _attributes["friendly_name"]

                            if _entity_id not in statistics:
                                statistics[_entity_id] = {friendly_name: 1}
                            elif friendly_name not in statistics[_entity_id]:
                                statistics[_entity_id][friendly_name] = 1
                                print("Found new name '%s' for entity '%s'. All names known so far: %s" % (
                                        friendly_name, _entity_id, statistics[_entity_id].keys()))
                                print(row)
                            else:
                                statistics[_entity_id][friendly_name] += 1

                        influx_batch_json.append(data)
                        influx_batch_size_cur += 1

                        if influx_batch_size_cur >= influx_batch_size_max:
                            if not args.dry:
                                influx.write(influx_batch_json)
                            influx_batch_size_cur = 0
                            influx_batch_json = []
            except Error as mysql_error:
                print(f"MySQL error on row {row_counter}: {mysql_error}")
                continue
            progress_bar.close()
            cursor.close()

    if not args.dry:
        influx.write(influx_batch_json)
    # Clean up by closing influx connection, and removing temporary table
    influx.close()
    if args.table == 'both':
        cursor = connection.cursor()
        remove_tmp_table(cursor)
        cursor.close()

    # print statistics - ideally you have one friendly name per entity_id
    # you can use the output to see where the same sensor has had different
    # names, as well as which entities do not have lots of measurements and
    # thus could be ignored (add them to exclude/entities in the influxdb yaml)
    for entity in sorted(statistics.keys()):
        print(entity)
        for friendly_name in sorted(statistics[entity].keys()):
            count = statistics[entity][friendly_name]
            print("  - %s (%d)" % (friendly_name, count))


def get_tables(table_key: str) -> list:
    """
    Switch case statement for tables
    """
    switcher = {
            'states': ['states'],
            'statistics': ['statistics'],
            'both': ['statistics', 'states']
    }
    try:
        return switcher[table_key]
    except KeyError:
        print("ERROR: argument --table should be \"states\" or \"statistics\"")


def formulate_sql_query(table: str, arg_tables: str):
    """
    Retrieves data from the HA databse
    """
    sql_query = ""
    if table == "states":
        # Using two different SQL queries in a Union to support data made with older HA db schema:
        # https://github.com/home-assistant/core/pull/71165
        sql_query = """select states.entity_id,
                              states.state,
                              states.attributes,
                              events.event_type as event_type,
                              events.time_fired as time_fired
                       from states,
                            events
                       where events.event_id = states.event_id
                       UNION
                       select states.entity_id,
                              states.state,
                              state_attributes.shared_attrs as attributes,
                              'state_changed',
                              states.last_updated as time_fired
                       from states, state_attributes
                       where event_id is null
                        and states.attributes_id = state_attributes.attributes_id;"""
    elif table == "statistics":
        if arg_tables == 'both':
            # If we're adding both, we should not add statistics for the same time period we're adding events
            inset_query = f"{sql_query}" + \
                f"\n         AND statistics.start < (select min(events.time_fired) as datetetime_start from events)"
        else:
            inset_query = ''
        sql_query = f"""
        SELECT statistics_meta.statistic_id,
               statistics.mean,
               statistics.min,
               statistics.max,
               state_attributes.shared_attrs,
               'state_changed',
               statistics.start
        FROM statistics_meta,
             statistics,
             state_attributes
        WHERE statistics.metadata_id = statistics_meta.id
        """ + \
         f""" AND mean != 0 {inset_query}
         AND state_attributes.attributes_id = (
            SELECT state_tmp.attributes_id
            FROM state_tmp
            WHERE state_tmp.entity_id = statistics_meta.statistic_id)
        """
    return sql_query


def formulate_tmp_table_sql():
    """
    Create temporary LUT to map attributes_id to entity_id to improve performance.

    TODO Not perfect solution, some entities have multiple attributes that change over time.
    TODO Here we select the most recent
    """
    return """CREATE TEMPORARY TABLE IF NOT EXISTS state_tmp AS
    SELECT max(states.attributes_id) as attributes_id, states.entity_id
    FROM states
    WHERE states.attributes_id IS NOT NULL
    GROUP BY states.entity_id;
    """


def remove_tmp_table(cursor):
    cursor.execute("""DROP TABLE IF EXISTS state_tmp;""")


if __name__ == "__main__":
    main()
