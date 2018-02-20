#include "database.h"
#include <sstream>
#include <occi.h>

using namespace oracle::occi;
using namespace std;

Database::Database(string hostAddress, int port, string dbName, string user, string password) {
    //[//]host[:port][/service name]
    
    stringstream ss;
    ss << "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=" << hostAddress << ")(PORT=" << port << ")) (CONNECT_DATA=(SERVICE_NAME=" << dbName << ")))";
    //ss << "//" << hostAddress << ":" << port << "/" << dbName;
    string db = ss.str();
    
    cout << user << "\n";
    cout << password << "\n";
    cout << db << "\n";
    
    env = Environment::createEnvironment(Environment::DEFAULT);
    conn = env->createConnection(user, password, db);
}

Database::~Database() {
    env->terminateConnection(conn);
    Environment::terminateEnvironment(env);
}

/**
 * Insertion of a row with dynamic binding, PreparedStatement functionality.
 */
void Database::insertBind(int c1, string c2) {
    string sqlStmt = "INSERT INTO author_tab VALUES (:x, :y)";
    stmt = conn->createStatement(sqlStmt);
    try {
        stmt->setInt(1, c1);
        stmt->setString(2, c2);
        stmt->executeUpdate();
        cout << "insert - Success" << endl;
    } catch (SQLException ex) {
        cout << "Exception thrown for insertBind" << endl;
        cout << "Error number: " << ex.getErrorCode() << endl;
        //cout << ex.getMessage() << endl;
    }

    conn->terminateStatement(stmt);
}

/**
 * Inserting a row into the table.
 */
void Database::insertRow() {
    string sqlStmt = "INSERT INTO author_tab VALUES (111, 'ASHOK')";
    stmt = conn->createStatement(sqlStmt);
    try {
        stmt->executeUpdate();
        cout << "insert - Success" << endl;
    } catch (SQLException ex) {
        cout << "Exception thrown for insertRow" << endl;
        cout << "Error number: " << ex.getErrorCode() << endl;
        //cout << ex.getMessage() << endl;
    }

    conn->terminateStatement(stmt);
}

/**
 * updating a row
 */
void Database::updateRow(int c1, string c2) {
    string sqlStmt =
            "UPDATE author_tab SET author_name = :x WHERE author_id = :y";
    stmt = conn->createStatement(sqlStmt);
    try {
        stmt->setString(1, c2);
        stmt->setInt(2, c1);
        stmt->executeUpdate();
        cout << "update - Success" << endl;
    } catch (SQLException ex) {
        cout << "Exception thrown for updateRow" << endl;
        cout << "Error number: " << ex.getErrorCode() << endl;
        //cout << ex.getMessage() << endl;
    }

    conn->terminateStatement(stmt);
}

/**
 * deletion of a row
 */
void Database::deleteRow(int c1, string c2) {
    string sqlStmt =
            "DELETE FROM author_tab WHERE author_id= :x AND author_name = :y";
    stmt = conn->createStatement(sqlStmt);
    try {
        stmt->setInt(1, c1);
        stmt->setString(2, c2);
        stmt->executeUpdate();
        cout << "delete - Success" << endl;
    } catch (SQLException ex) {
        cout << "Exception thrown for deleteRow" << endl;
        cout << "Error number: " << ex.getErrorCode() << endl;
        //cout << ex.getMessage() << endl;
    }

    conn->terminateStatement(stmt);
}

void Database::loadBinaryTree(/*BinaryTree* binaryTree, */string hashingRefs, string videoTimeBeginString, string videoTimeEndString, int hashWindow) throw(runtime_error) {
    /*PGconn* conn = this->connect();
    PGresult* res;
    int i;
    
    if(!conn) {
        throw runtime_error("CONNECTION_FAILED");
    }*/

    string query = getVHCQuery(hashingRefs, videoTimeBeginString, videoTimeEndString, hashWindow);
    cout << "QUERY >> " << query << "\n";
    
    stmt = conn->createStatement(query);
    ResultSet* rset = stmt->executeQuery();
    try {
        while(rset->next()) {
            uint32_t time = rset->getUInt(1);
            uint64_t vhc = (unsigned long) rset->getNumber(2).operator unsigned long();
            string extRef = rset->getString(3);
            uint64_t timeRef = (unsigned long) rset->getNumber(4).operator unsigned long();
            
            cout <<
                "time: " << time << "\n" <<
                "vhc: " << vhc << "\n" <<
                "extRef: " << extRef << "\n" <<
                "timeRef: " << timeRef << "\n\n";
        }
    }
    catch(SQLException ex) {
        cout << "Exception thrown for displayElements" << endl;
        cout << "Error number: " << ex.getErrorCode() << endl;
        //cout << ex.getMessage() << endl;
    }
        
    stmt->closeResultSet(rset);
    conn->terminateStatement(stmt);

    // Close the connection to the database and cleanup
    //PQfinish(conn);
}

void Database::loadBinaryTree(/*BinaryTree* binaryTree, */string hashingRefs, string videoTimeBeginString, string videoTimeEndString, int hashWindow, uint8_t prefix) throw(runtime_error) {
    /*PGconn* conn = this->connect();
    PGresult* res;
    int i;
    
    if(!conn) {
        throw runtime_error("CONNECTION_FAILED");
    }*/
    
    //string query = getVHCQueryWithBounds(hashingRefs, videoTimeBeginString, videoTimeEndString, hashWindow, prefix);
    string query = getVHCQuery(hashingRefs, videoTimeBeginString, videoTimeEndString, hashWindow);
    cout << "QUERY >> " << query << "\n";

    stmt = conn->createStatement(query);
    ResultSet* rset = stmt->executeQuery();
    try {
        while(rset->next()) {
            uint32_t time = rset->getUInt(1);
            uint64_t vhc = (unsigned long) rset->getNumber(2).operator unsigned long();
            string extRef = rset->getString(3);
            uint64_t timeRef = (unsigned long) rset->getNumber(4).operator long();
            
            cout <<
                "time: " << time << "\n" <<
                "vhc: " << vhc << "\n" <<
                "extRef: " << extRef << "\n" <<
                "timeRef: " << timeRef << "\n\n";
        }
    }
    catch(SQLException ex) {
        cout << "Exception thrown for displayElements" << endl;
        cout << "Error number: " << ex.getErrorCode() << endl;
        //cout << ex.getMessage() << endl;
    }
        
    stmt->closeResultSet(rset);
    conn->terminateStatement(stmt);

    // Close the connection to the database and cleanup
    //PQfinish(conn);
}

/*list<VHash*>**/ void Database::loadHashes(string hashingRefs, string begin, string end, int hashWindow) {
    /*PGconn* conn = this->connect();
    PGresult* res;
    int i;
    
    if(!conn) {
        return 0;
    }*/
    
    string query = getVHCQuery(hashingRefs, begin, end, hashWindow);
    cout << "QUERY >> " << query << "\n";

    //list<VHash*>* hashList = new list<VHash*>();
    stmt = conn->createStatement(query);
    ResultSet* rset = stmt->executeQuery();
    try {
        while(rset->next()) {
            uint32_t time = rset->getUInt(1);
            uint64_t vhc = (unsigned long) rset->getNumber(2);
            string extRef = rset->getString(3);
            uint64_t timeRef = (unsigned long) rset->getNumber(4);
            
            cout <<
                "time: " << time << "\n" <<
                "vhc: " << vhc << "\n" <<
                "extRef: " << extRef << "\n" <<
                "timeRef: " << timeRef << "\n\n";
            
            //hashList->push_back(new VHash(timeRef+time, vhc, extRef));
        }
    }
    catch(SQLException ex) {
        cout << "Exception thrown for displayElements" << endl;
        cout << "Error number: " << ex.getErrorCode() << endl;
        //cout << ex.getMessage() << endl;
    }
        
    stmt->closeResultSet(rset);
    conn->terminateStatement(stmt);
    
    return;
    //return hashList;
}

void Database::split(string s, char delim, vector<string> *elems) {
    stringstream ss(s);
    string item;

    while(getline(ss, item, delim)) {
        elems->push_back(item);
    }
}
string Database::getVHCQuery(string hashingRefs, string beginString, string endString, int hashWindow) {
    stringstream query;
    query << "SELECT vhc.hash_time,vhc.vhc,hashing.ext_ref,hashing.time_ref FROM vsr_vhc,vsr_hashing WHERE vhc.hashing_id=hashing.id";
    
    vector<string> hashingRefVector;
    split(hashingRefs, ',', &hashingRefVector);
    
    //Filtro por nome de canal ou id de obra
    if(!hashingRefVector.empty()) {
        vector<string>::iterator it = hashingRefVector.begin();
        
        query << " AND (" << "hashing.ext_ref='" << (*it) << "'";
        it++;
        
        for( ; it != hashingRefVector.end(); it++) {
            query << " OR hashing.ext_ref='" << (*it) << "'";
        }
        
        query << ")";
    }

    //Filtro temporal (início)
    if(!beginString.empty()) {
        query << " AND hashing.time_ref+vhc.hash_time>='" << beginString << "'";
    }
    
    //Filtro temporal (fim)
    if(!endString.empty()) {
        query << " AND hashing.time_ref+vhc.hash_time+" << hashWindow << "<='" << endString << "'";
    }
    
    //FIXME
    query << " ORDER BY vhc.vhc";
    
    return query.str();
}