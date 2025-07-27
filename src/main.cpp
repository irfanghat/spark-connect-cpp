#include "client.h"
#include "config.h"

int main()
{
    Config conf;
    conf.setHost("localhost").setPort(15002);

    SparkClient client(conf);

    auto df = client.sql("SELECT * FROM range(10)");
    df.show(5);
}
