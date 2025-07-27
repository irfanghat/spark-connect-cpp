#include "client.h"
#include "config.h"

int main()
{
    Config conf;
    conf.setHost("localhost").setPort(15002);

    SparkClient client(conf);

    auto df1 = client.sql("SELECT * FROM range(1000)");
    df1.show(5);

    auto df2 = client.sql("SELECT 'John' AS name");
    df2.show();
}
