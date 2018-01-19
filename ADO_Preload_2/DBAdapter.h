#pragma once
#import "C:\Program Files\Common Files\System\ado\msado15.dll" no_namespace rename("EOF", "adoEOF")
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#define ROW_SIZW 6
#define MAX_LENGTH 255

class DBAdapter {
private:
	_ConnectionPtr pConnection;
	_CommandPtr pCommand;
	_RecordsetPtr pRecordset;

	void create_connection();
	void close_connection();
	//void create_command();
	//void close_command();
	void create_recordset();
	void close_recordset();
public:
	DBAdapter() = default;
	~DBAdapter() = default;

public:
	void initialize();
	void uninitialize();
	// MySQL overload
	void make_connection(std::string host_name,
		std::string port_name,
		std::string db_name,
		std::string user,
		std::string password);
	// MySQL overload
	void make_connection(std::string host_name,
		std::string port_name,
		std::string db_name,
		std::string schema_name,
		std::string user,
		std::string password);
	void excute_sql(std::string conn_str);
	void show_SQL_result();
	bool has_record();
	void fetch_row(char (&row)[ROW_SIZW][MAX_LENGTH]);
	// debug
	_RecordsetPtr getRecordset() { return this->pRecordset; }
};

