#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "typedb_driver.h"

#define SERVER_ADDR "127.0.0.1:1729"
#define DB_NAME "sample_app_db"
#define CLOUD_USERNAME "admin"
#define CLOUD_PASSWORD "password"
#define FAILED() check_error_may_print(__FILE__, __LINE__)

typedef enum { CORE, CLOUD } edition;
edition TYPEDB_EDITION = CORE;

// Utility functions to handle errors and file operations
void handle_error(const char* message) {
    fprintf(stderr, "%s\n", message);
    exit(EXIT_FAILURE);
}

void executeQuery(Transaction* tx, const char* query) {
    if (FAILED()) handle_error("Transaction failed to start.");
    void_promise_resolve(query_define(tx, query, NULL));
    if (FAILED()) handle_error("Query execution failed.");
    void_promise_resolve(transaction_commit(tx));
    if (FAILED()) handle_error("Transaction commit failed.");
    transaction_close(tx);
}

// Setup schema and data for the database
void dbSchemaSetup(Session* schemaSession, const char* schemaFile) {
    char defineQuery[4096] = {0};
    FILE* file = fopen(schemaFile, "r");
    if (!file) handle_error("Failed to open schema file.");

    char line[512];
    while (fgets(line, sizeof(line), file)) strcat(defineQuery, line);
    fclose(file);

    Transaction* tx = transaction_new(schemaSession, Write, NULL);
    executeQuery(tx, defineQuery);
    printf("Schema setup complete.\n");
}

void dbDatasetSetup(Session* dataSession, const char* dataFile) {
    char insertQuery[4096] = {0};
    FILE* file = fopen(dataFile, "r");
    if (!file) handle_error("Failed to open data file.");

    char line[512];
    while (fgets(line, sizeof(line), file)) strcat(insertQuery, line);
    fclose(file);

    Transaction* tx = transaction_new(dataSession, Write, NULL);
    executeQuery(tx, insertQuery);
    printf("Dataset setup complete.\n");
}

bool createDatabase(DatabaseManager* dbManager, const char* dbName) {
    printf("Creating new database: %s\n", dbName);
    databases_create(dbManager, dbName);
    if (FAILED()) handle_error("Database creation failed.");

    Session* schemaSession = session_new(dbManager, dbName, Schema, NULL);
    dbSchemaSetup(schemaSession, "iam-schema.tql");
    session_close(schemaSession);

    Session* dataSession = session_new(dbManager, dbName, Data, NULL);
    dbDatasetSetup(dataSession, "iam-data-single-query.tql");
    session_close(dataSession);

    return true;
}

// Connecting to TypeDB based on edition
Connection* connectToTypeDB(edition typedb_edition, const char* addr) {
    Connection* connection = NULL;
    if (typedb_edition == CORE) {
        connection = connection_open_core(addr);
    } else {
        connection = connection_open_cloud(addr, CLOUD_USERNAME, CLOUD_PASSWORD, true);
    }
    if (!connection) handle_error("Failed to connect to TypeDB server.");
    return connection;
}

int main() {
    Connection* connection = connectToTypeDB(TYPEDB_EDITION, SERVER_ADDR);
    DatabaseManager* dbManager = database_manager_new(connection);
    if (!dbManager) handle_error("Failed to get database manager.");

    if (!createDatabase(dbManager, DB_NAME)) handle_error("Failed to set up the database.");

    // Further functionalities should be implemented here as per the original C++ example
    database_manager_drop(dbManager);
    connection_close(connection);
    return EXIT_SUCCESS;
}
