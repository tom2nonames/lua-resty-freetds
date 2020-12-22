package t::Test;

#use strict;
#use warnings;

use Test::Nginx::Socket::Lua::Stream -Base;
use Cwd qw(cwd);

repeat_each(1);
log_level('debug');
no_long_string();
no_shuffle();

no_root_location();

worker_connections(128);

my $pwd = cwd();

my $default_config = qq{
    resolver \$TEST_NGINX_RESOLVER;
    lua_package_path "$pwd/t/servroot/html/?.lua;$pwd/lib/?.lua;$pwd/t/lib/?.lua;$pwd/../lua-resty-rsa/lib/?.lua;$pwd/../lua-resty-string/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_code_cache on;
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_MSSQL_PORT} ||= 1433;
$ENV{TEST_NGINX_MSSQL_HOST} ||= '10.66.60.42';
$ENV{TEST_NGINX_MYSQL_PATH} ||= '/var/run/mysql/mysql.sock';

no_long_string();

add_block_preprocessor(sub {
    my $block = shift;

    if (defined($ENV{TEST_SUBSYSTEM}) && $ENV{TEST_SUBSYSTEM} eq "stream") {
        if (!defined $block->stream_config) {
            $block->set_value("stream_config", $default_config);
        }
        if (!defined $block->stream_server_config) {
            $block->set_value("stream_server_config", $block->server_config);
        }
    } else {
        if (!defined $block->http_config) {
            $block->set_value("http_config", $default_config);
        }
        if (!defined $block->request) {
            $block->set_value("request", "GET /t\n");
        }
        if (!defined $block->config) {
            $block->set_value("config", $block->server_config );
        }
    }
});

1;