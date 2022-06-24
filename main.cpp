#include "nameServer.h"

const int numReplicate = 3;
const int ChunkSize = 2 * 1024 * 1024;


int main() {
    NameServer ns(numReplicate, ChunkSize);
    ns.creatDataServers(numReplicate + 1);
    ns();
}