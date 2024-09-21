# Once:
#     docker pull mysql
#
# Enter 
#     mysql sandbox --password=password
#     mysql sandbox --user utente --password=password

/*

Errore "Public Key Retrieval is not allowed" in connessione:

https://stackoverflow.com/questions/50379839/connection-java-mysql-public-key-retrieval-is-not-allowed

"You should add client option to your mysql-connector allowPublicKeyRetrieval=true to allow 
the client to automatically request the public key from the server. 
Note that allowPublicKeyRetrieval=True could allow a malicious proxy to 
perform a MITM attack to get the plaintext password, so it is False by default 
and must be explicitly enabled."


https://medium.com/@kiena/troubleshooting-docker-and-dbeaver-connection-access-denied-for-user-172-17-0-1-using-18597addbe33


*/


docker run \
    --name multiprocessing-executor-demo \
    -e MYSQL_ROOT_PASSWORD=password \
    -e MYSQL_DATABASE=multiprocessing-executor-demo \
    -v ./init.d:/docker-entrypoint-initdb.d \
    -p 3306:3306 \
    -it -d mysql:latest 