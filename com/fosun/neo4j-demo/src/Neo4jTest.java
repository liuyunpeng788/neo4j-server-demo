import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jTest implements AutoCloseable {
    private final Driver driver;

    public Neo4jTest(String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    public Neo4jTest(String uri, String user, String password,int poolSize){
        if(poolSize > 1){
            driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password )
                    ,Config.build()
                            .withMaxConnectionPoolSize(poolSize)
                            .withConnectionLivenessCheckTimeout(1, TimeUnit.SECONDS)
                            .withConnectionTimeout(1,TimeUnit.HOURS)
                            .withConnectionAcquisitionTimeout(5, TimeUnit.SECONDS)
                            .withLoadBalancingStrategy(Config.LoadBalancingStrategy.LEAST_CONNECTED)
                            .withMaxTransactionRetryTime(30,TimeUnit.MILLISECONDS)
                            .toConfig());
        }else{
            driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
        }
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    // Create an employment relationship to a pre-existing company node.
    // This relies on the person first having been created.
    private StatementResult employ( final Transaction tx, final String person, final String company )
    {
        return tx.run( "MATCH (person:Person {name: $person_name}) " +
                        "MATCH (company:Company {name: $company_name}) " +
                        "CREATE (person)-[:WORKS_FOR]->(company)",
                parameters( "person_name", person, "company_name", company ) );
    }

    // Create a friendship between two people.
    private StatementResult makeFriends( final Transaction tx, final String person1, final String person2 )
    {
        return tx.run( "MATCH (a:Person {name: $person_1}) " +
                        "MATCH (b:Person {name: $person_2}) " +
                        "MERGE (a)-[:KNOWS]->(b)",
                parameters( "person_1", person1, "person_2", person2 ) );
    }

    /**
     * 获取所有的结果
     * @param message 查询的属性值
     * @return 所有节点信息
     */
    public List<Record> getResult(final String message){
        try(Session session = driver.session()){
            List<Record> res = session.readTransaction((Transaction tx)-> {
                        List<Record> list = new ArrayList<>();
                        StatementResult statement = tx.run("MATCH(p:Greeting{message:$message}) return p.message as msg ", parameters("message", message));
                        while (statement.hasNext()) {
                            Map<String,Object> record = statement.next().asMap();
                            for(Map.Entry<String,Object> entry: record.entrySet()){
                                System.out.println("key:" + entry.getKey() + " value:" + entry.getValue());
                            }
                            System.out.println("--------------------");
                        }
                        return list;
                    });
            return res;
        }
    }
    public void printGreeting( final String message )
    {
        try ( Session session = driver.session() )
        {

            /**
             *  这个地方可以用lambda 表达式简写为
             *  String greeting = session.writeTransaction((Transaction tx)->{ // execute 函数体});
             */
            /
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MERGE(a:Greeting) " +
                                    " ON CREATE SET a.message = $message " +
                                    " ON MATCH SET a.message = $message " +
                                    "RETURN a.message + ', from node ' + id(a)",
                            parameters( "message", message ) );

                    return result.single().get( 0 ).asString();
                }
            } );
            System.out.println( greeting );
        }
    }

    public static void main( String... args ) throws Exception
    {
        try ( Neo4jTest greeter = new Neo4jTest( "bolt://fonova-hadoop5:7687", "neo4j", "neo4j" ,10) ) {
            greeter.getResult("hello, world");
            greeter.printGreeting("hello, world");
        }
    }
}
