# STRIP strips color from terminal output
STRIP="perl -pe 's/\e\[?.*?[\@-~]//g'"

# TODO use a for loop like a grownup
docker logs dockertest_bootstrap_1 2>&1 | eval $STRIP > ./build/bootstrap.log 
docker logs dockertest_client_1 2>&1 | eval $STRIP > ./build/client.log 
docker logs dockertest_data_1 2>&1 | eval $STRIP > ./build/data.log 
docker logs dockertest_server_1 2>&1 | eval $STRIP > ./build/server.log

docker cp dockertest_server_1:/root/.go-ipfs/logs/events.log    ./build/server-events.log
docker cp dockertest_bootstrap_1:/root/.go-ipfs/logs/events.log ./build/bootstrap-events.log
docker cp dockertest_client_1:/root/.go-ipfs/logs/events.log    ./build/client-events.log
