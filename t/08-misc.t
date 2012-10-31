#!perl

use strict;
use warnings;

use Test::More;
use Test::Differences;
use Test::Exception;
use ElasticSearch::SearchBuilder;

my $a = ElasticSearch::SearchBuilder->new;

eq_or_diff $a->query( { k => { '=' => { query => 0 } } } ),
    { query => { match => { k => { query => 0 } } } },
    'False string hash_param';

throws_ok { $a->query( { k => { '=' => { query => undef } } } ) }
qr/missing required param/, 'Undefined hash_param';

done_testing;
