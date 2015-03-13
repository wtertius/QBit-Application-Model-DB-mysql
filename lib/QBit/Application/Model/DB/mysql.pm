package QBit::Application::Model::DB::mysql;

use qbit;

use base qw(QBit::Application::Model::DB);

use QBit::Application::Model::DB::mysql::Table;
use QBit::Application::Model::DB::mysql::Query;
use QBit::Application::Model::DB::Filter;

eval {require Exception::DB::DuplicateEntry};

sub filter {
    my ($self, $filter, %opts) = @_;

    return QBit::Application::Model::DB::Filter->new($filter, %opts, db => $self);
}

sub query {
    my ($self) = @_;

    return QBit::Application::Model::DB::mysql::Query->new(db => $self);
}

sub transaction {
    my ($self, $sub) = @_;

    $self->_connect();
    local $self->{'__DBH__'}{$$}{'mysql_auto_reconnect'} = FALSE;

    $self->SUPER::transaction($sub);
}

sub _do {
    my ($self, $sql, @params) = @_;

    my $res;
    try {
        $res = $self->SUPER::_do($sql, @params);
    }
    catch Exception::DB with {
        my $e = shift;
        $e->{'text'} =~ /^Duplicate entry/
          ? throw Exception::DB::DuplicateEntry $e
          : throw $e;
    };

    return $res;
}

sub _get_table_class {
    my ($self, %opts) = @_;

    my $table_class;
    if (defined($opts{'type'})) {
        my $try_class = "QBit::Application::Model::DB::mysql::Table::$opts{'type'}";
        $table_class = $try_class if eval("require $try_class");

        throw gettext('Unknown table class "%s"', $opts{'type'}) unless defined($table_class);
    } else {
        $table_class = 'QBit::Application::Model::DB::mysql::Table';
    }

    return $table_class;
}

sub _create_sql_db {
    my ($self) = @_;

    return
        'CREATE DATABASE '
      . $self->{'__DBH__'}{$$}->quote_identifier($self->get_option('database'))
      . "\nDEFAULT CHARACTER SET UTF8;\n" . 'USE '
      . $self->{'__DBH__'}{$$}->quote_identifier($self->get_option('database')) . ";\n\n";
}

sub _connect {
    my ($self) = @_;

    unless (defined($self->{'__DBH__'}{$$})) {
        my $dsn = 'DBI:mysql:'
          . join(
            ';', map {$_ . '=' . $self->get_option($_)}
              grep {defined($self->get_option($_))} qw(database host port)
          );

        $self->{'__DBH__'}{$$} = DBI->connect(
            $dsn,
            $self->get_option('user',     ''),
            $self->get_option('password', ''),
            {
                PrintError           => 0,
                RaiseError           => 0,
                AutoCommit           => 1,
                mysql_auto_reconnect => 1,
                mysql_enable_utf8    => 1,
            },
        ) || throw DBI::errstr();
    }
}

sub _is_connection_error {
    my ($self, $code) = @_;

    return in_array($code || 0, [2006]);
}

TRUE;
