#include <iostream>
#include <occi.h>
#include "database.h"

using namespace oracle::occi;
using namespace std;

int main() {
    try {
        printf("111\n");
        Database database("192.168.100.10", 1521, "xe", "vhcservice", "vhcservice");
        printf("222\n");
        database.loadHashes("GD", "", "", 30);
        printf("333\n");
    }
    /*catch(SQLException ex) {
        cout << ex.getMessage() << endl;
    }*/
    catch(runtime_error& e) {
        printf("ERROR %s\n", e.what());
        fflush(stdout);
    }
    cout << "occidml - done" << endl;
    
    return 0;
}