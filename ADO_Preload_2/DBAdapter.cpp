#include <map>
#include <string.h>
#include "DBAdapter.h"

#include <locale>

// create ADO connection
inline void DBAdapter::create_connection() {
	HRESULT hr = pConnection.CreateInstance(__uuidof(Connection));
	if (FAILED(hr))
	{
		printf("ADO connection creation failed\n");
		exit(-1);
	}
	// std::cout << "pConnection state: " << (int)pConnection->State << std::endl;
}

inline void DBAdapter::close_connection() {
	try {
		pConnection->Close();
	}
	catch (_com_error e) {
		std::cout << e.Description() << std::endl;
	}
}

// create ADO Recordset
inline void DBAdapter::create_recordset() {
	HRESULT hr = pRecordset.CreateInstance(__uuidof(Recordset));
	if (FAILED(hr))
	{
		printf("ADO Recordset creation failed\n");
		exit(-1);
	}
	// std::cout << "pRecordset state: " << (int)pConnection->State << std::endl;
}

inline void DBAdapter::close_recordset() {
	try {
		pRecordset->Close();
	}
	catch (_com_error e) {
		std::cout << e.Description() << std::endl;
	}
}

void DBAdapter::initialize() {
	// initialize COM environment
	HRESULT hr = ::CoInitialize(NULL);
	if (FAILED(hr))
	{
		printf("COM initialization failed\n");
		exit(-1);
	}
	create_connection();
	create_recordset();
}

void DBAdapter::uninitialize() {
	// release connection
	close_recordset();
	close_connection();
	::CoUninitialize();
}

// MYSQL overload
void DBAdapter::make_connection(
	std::string host_name, 
	std::string port_name, 
	std::string db_name,
	std::string user, 
	std::string password)
{
	try {
		pConnection->ConnectionTimeout = 600;
		pConnection->CommandTimeout = 120;
		pConnection->ConnectionString = ("Driver={MySQL ODBC 5.3 Unicode Driver};"
			"Server=" + host_name + ";"
			"Port=" + port_name + ";"
			"Database=" + db_name + ";"
			"User=" + user + ";"
			"Password=" + password + ";"
			// "charset=GBK;"
			"Option=3;").c_str();
		pConnection->Open("", "", "", adConnectUnspecified);
		// std::cout << "pConnection state: " << pConnection->State << std::endl;
	}
	catch (_com_error e) {
		std::cout << e.Description() << std::endl;
	}
}

// postgreSQL overload
void DBAdapter::make_connection(
	std::string host_name,
	std::string port_name,
	std::string schema_name,
	std::string db_name,
	std::string user,
	std::string password)
{
	try {
		pConnection->ConnectionTimeout = 600;
		pConnection->CommandTimeout = 120;
		pConnection->ConnectionString = ("Driver={PostgreSQL Unicode(x64)};"
			"Server=" + host_name + ";"
			"Port=" + port_name + ";"
			"Schema=" + schema_name +";"
			"Database=" + db_name + ";"
			"Uid=" + user + ";"
			"Pwd=" + password + ";"
			"charset=GBK;").c_str();
		pConnection->Open("", "", "", adConnectUnspecified);
		// std::cout << "pConnection state: " << pConnection->State << std::endl;
	}
	catch (_com_error e) {
		std::cout << e.Description() << std::endl;
	}
}

void DBAdapter::excute_sql(std::string conn_str) {
	try {
		// the select sql
		pRecordset = pConnection->
			Execute(conn_str.c_str(), NULL, adCmdText);
		// std::cout << "Recordset state: " << pRecordset->State << std::endl;
	}
	catch (_com_error e) {
		std::cout << e.Description() << std::endl;
	}
}

inline bool DBAdapter::has_record() {
	return !pRecordset->adoEOF;
}

void DBAdapter::fetch_row(char (&row)[ROW_SIZE][MAX_LENGTH])
{
	try {
		_variant_t index;
		index.vt = VT_I2;
		char ctmp[MAX_LENGTH] = { 0 };
		for (long i = 0; i < pRecordset->Fields->Count; ++i) {
			index.intVal = i;
			auto field = pRecordset->Fields->GetItem(index);
			// null check
			auto field_value = field->GetValue();
			if (field_value.vt == VT_NULL)
				row[i][0] = 0;
			else {
				strcpy(ctmp,(char*)_bstr_t(field_value));
				strcpy(row[i], ctmp);
			}

			// cout << ctmp << " ";
			//wcscpy(wctmp, (wchar_t*)_bstr_t(field->GetValue()));
			//WideCharToMultiByte(CP_ACP, WC_COMPOSITECHECK, wctmp, -1, ctmp, sizeof(ctmp), NULL, NULL);
			//setlocale(LC_ALL, "zh_CN.UTF-8");
			//wprintf(L"%ls ", wctmp);
		}

		pRecordset->MoveNext();
	}
	catch (_com_error e) {
		std::cout << e.Description() << std::endl;
	}
}

// this function is just for testing, deleting it won't incur any problem
void DBAdapter::show_SQL_result() {
	try {
		using std::cout;
		using std::endl;
		using std::string;
		using std::wstring;

		// loop
		int count = 0;
		while (has_record()) {
			++count;
			if (count % 1000 == 0) cout << "count = " << count << endl;
			_bstr_t bstrVal;
			_variant_t index;
			index.vt = VT_I2;
			for (long i = 0; i < pRecordset->Fields->Count; ++i)
			{
				index.intVal = i;
				// pRecordset->Fields->get_Item(vIntegerType, &pvObject);
				auto pvObject = pRecordset->Fields->GetItem(index);
				bstrVal = pvObject->GetValue();
				wstring ws = (wchar_t*)bstrVal;
				string s(ws.begin(), ws.end());
			}
			// cout << endl;
			pRecordset->MoveNext();
		}
	}
	catch (_com_error e) {
		std::cout << e.Description() << std::endl;
	}
}