package DBIx::OpenTracing;
use strict;
use warnings;
use feature qw[ state ];
use syntax 'maybe';
use B;
use Carp qw[ croak ];
use DBI;
use DBIx::OpenTracing::Constants ':ALL';
use List::Util qw[ sum ];
use OpenTracing::GlobalTracer;
use Package::Constants;
use Scalar::Util qw[ blessed ];
use Scope::Context;

our $VERSION = 'v0.0.3';

use constant TAGS_DEFAULT => (DB_TAG_TYPE ,=> 'sql');

use constant {
    _DBI_EXECUTE            => \&DBI::st::execute,
    _DBI_DO                 => \&DBI::db::do,
    _DBI_SELECTALL_ARRAYREF => \&DBI::db::selectall_arrayref,
    _DBI_SELECTROW_ARRAYREF => \&DBI::db::selectrow_arrayref,
    _DBI_SELECTROW_ARRAY    => \&DBI::db::selectrow_array,
};
use constant _PP_MODE => !!$INC{'DBI/PurePerl.pm'};

if (%DBIx::QueryLog::SKIP_PKG_MAP) {    # hide from DBIx::QueryLog's caller()
    $DBIx::QueryLog::SKIP_PKG_MAP{ (__PACKAGE__) } = 1;
}

my ($is_enabled, $is_suspended);

sub _numeric_result { 0 + $_[0] }
sub _array_size     { scalar @{ $_[0] } }

sub enable {
    return if $is_enabled or $is_suspended;

    state $do                 = _gen_wrapper(_DBI_DO, \&_numeric_result);
    state $selectall_arrayref = _gen_wrapper(_DBI_SELECTALL_ARRAYREF, \&_array_size);
    state $selectrow_arrayref = _gen_wrapper(_DBI_SELECTROW_ARRAYREF);
    state $selectrow_array    = _gen_wrapper(_DBI_SELECTROW_ARRAY);
 
    no warnings 'redefine';
    *DBI::st::execute = \&_execute;

    if (not _PP_MODE) {    # everything goes through execute() in PP mode
        *DBI::db::do                 = $do;
        *DBI::db::selectall_arrayref = $selectall_arrayref;
        *DBI::db::selectrow_arrayref = $selectrow_arrayref;
        *DBI::db::selectrow_array    = $selectrow_array;
    }
 
    $is_enabled = 1;

    return;
}

sub disable {
    return unless $is_enabled;

    no warnings 'redefine';
    *DBI::st::execute = _DBI_EXECUTE;

    if (not _PP_MODE) {
        *DBI::db::do                 = _DBI_DO;
        *DBI::db::selectall_arrayref = _DBI_SELECTALL_ARRAYREF;
        *DBI::db::selectrow_arrayref = _DBI_SELECTROW_ARRAYREF;
        *DBI::db::selectrow_array    = _DBI_SELECTROW_ARRAY;
    }
 
    $is_enabled = 0;

    return;
}

sub import {
    my ($class, $tag_mode) = @_;

    enable();
    return if not defined $tag_mode;

    my @sensitive_tags = (
        DB_TAG_SQL,
        DB_TAG_BIND,
        DB_TAG_USER,
        DB_TAG_DBNAME,
    );

    if ($tag_mode eq '-empty') {
        $class->hide_tags(DB_TAGS_ALL);
    }
    elsif ($tag_mode eq '-safe') {
        $class->hide_tags(@sensitive_tags);
    }
    elsif ($tag_mode eq '-secure') {
        $class->_disable_tags(@sensitive_tags);
    }
    else {
        croak "Unknown mode: $tag_mode";
    }
    return;
}

sub unimport { disable() }

sub _tags_dbh {
    my ($dbh) = @_;
    return (
        maybe
        DB_TAG_USER   ,=> $dbh->{Username},
        DB_TAG_DBNAME ,=> $dbh->{Name},
    );
}

sub _tags_sth {
    my ($sth) = @_;
    return (DB_TAG_SQL ,=> $sth) if !blessed($sth) or !$sth->isa('DBI::st');
    return (
        _tags_dbh($sth->{Database}),
        DB_TAG_SQL ,=> $sth->{Statement},
    );
}

sub _tags_bind_values {
    my ($bind_ref) = @_;
    return if not @$bind_ref;

    my $bind_str = join ',', map { "`$_`" } @$bind_ref;
    return (DB_TAG_BIND ,=> $bind_str);
}

{
    my (%hidden_tags, %disabled_tags);

    sub _filter_tags {
        my ($tags) = @_;
        delete @$tags{ keys %disabled_tags, keys %hidden_tags };
        return $tags;
    }

    sub _tag_enabled {
        my ($tag) = @_;
        return !!_filter_tags({ $tag => 1 })->{$tag};
    }

    sub hide_tags {
        my ($class, @tags) = @_;;
        return if not @tags;

        undef @hidden_tags{@tags};
        return;
    }

    sub show_tags {
        my ($class, @tags) = @_;
        return if not @tags;

        delete @hidden_tags{@tags};
        return;
    }

    sub hide_tags_temporarily {
        my $class = shift;
        my @tags  = grep { not exists $hidden_tags{$_} } @_;
        $class->hide_tags(@tags);
        Scope::Context->up->reap(sub { $class->show_tags(@tags) });
    }

    sub show_tags_temporarily {
        my $class = shift;
        my @tags = grep { exists $hidden_tags{$_} } @_;
        $class->show_tags(@tags);
        Scope::Context->up->reap(sub { $class->hide_tags(@tags) });
    }

    sub _disable_tags {
        my ($class, @tags) = @_;
        undef @disabled_tags{@tags};
        return;
    }

    sub _enable_tags {
        my ($class, @tags) = @_;
        delete @disabled_tags{@tags};
        return;
    }

    sub disable_tags {
        my $class = shift;
        my @tags  = grep { not exists $disabled_tags{$_} } @_;
        $class->_disable_tags(@tags);
        Scope::Context->up->reap(sub { $class->_enable_tags(@tags) });
    }
}

sub _add_tag {
    my ($span, $tag, $value) = @_;
    return unless _tag_enabled($tag);
    $span->add_tag($tag => $value);
}

sub _execute {
    my $sth = shift;
    my @bind = @_;
    
    my $tracer = OpenTracing::GlobalTracer->get_global_tracer();
    my $scope = $tracer->start_active_span(
        'dbi_execute',
        tags => _filter_tags({
            TAGS_DEFAULT,
            _tags_sth($sth),
            _tags_bind_values(\@bind)
        }),
    );
    my $span = $scope->get_span();

    my $result;
    my $failed = !eval { $result = $sth->${ \_DBI_EXECUTE }(@_); 1 };
    my $error  = $@;

    if ($failed or not defined $result) {
        $span->add_tag(error => 1);
    }
    elsif ($sth->{NUM_OF_FIELDS} == 0) {    # non-select statement
        _add_tag($span, DB_TAG_ROWS,=> $result +0);
    }
    $scope->close();

    die $error if $failed;
    return $result;
}

sub _gen_wrapper {
    my ($method, $row_counter) = @_;
    my $method_name = B::svref_2object($method)->GV->NAME;

    return sub {
        my $dbh = shift;
        my ($statement, $attr, @bind) = @_;

        my $tracer = OpenTracing::GlobalTracer->get_global_tracer();
        my $scope = $tracer->start_active_span("dbi_$method_name",
            tags => _filter_tags({
                TAGS_DEFAULT,
                _tags_sth($statement),
                _tags_dbh($dbh),
                _tags_bind_values(\@bind),
            }),
        );
        my $span = $scope->get_span();

        my $result;
        my $wantarray = wantarray;          # eval has its own
        my $failed    = !eval {
            if ($wantarray) {
                $result = [ $dbh->$method(@_) ];
            }
            else {
                $result = $dbh->$method(@_);
            }
            1;
        };
        my $error = $@;

        if ($failed or defined $dbh->err) {
            $span->add_tag(error => 1);
        }
        elsif (defined $row_counter) {
            my $rows = sum(map { $row_counter->($_) } $wantarray ? @$result : $result);
            _add_tag($span, DB_TAG_ROWS ,=> $rows);
        }
        $scope->close();

        die $error if $failed;
        return $wantarray ? @$result : $result;
    }
}

sub enable_temporarily {
    return if $is_enabled;

    enable();
    Scope::Context->up->reap(\&disable);
}

sub disable_temporarily {
    return unless $is_enabled;

    disable();
    Scope::Context->up->reap(\&enable);
}

sub suspend {
    return if $is_suspended;

    my $was_enabled = $is_enabled;
    disable();
    $is_suspended = 1;
    Scope::Context->up->reap(sub { $is_suspended = 0; enable() if $was_enabled });
}

1;
