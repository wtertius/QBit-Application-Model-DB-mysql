package QBit::Application::Model::DB::mysql::Table;

use qbit;

use base qw(QBit::Application::Model::DB::Table);

use QBit::Application::Model::DB::mysql::Field;

our $ADD_CHUNK = 1000;

sub create_sql {
    my ($self) = @_;

    throw gettext('Inherites does not realize') if $self->inherits;

    return
        'CREATE TABLE ' 
      . $self->quote_identifier($self->name) 
      . " (\n    "
      . join(
        ",\n    ",
        (map {$_->create_sql()} @{$self->fields}),
        (
            $self->primary_key
            ? 'PRIMARY KEY (' . join(', ', map {$self->quote_identifier($_)} @{$self->primary_key}) . ')'
            : ()
        ),
        (map {$self->_create_sql_index($_)} @{$self->indexes            || []}),
        (map {$self->_create_sql_foreign_key($_)} @{$self->foreign_keys || []}),
      )
      . "\n"
      . ") ENGINE='InnoDB' DEFAULT CHARACTER SET 'UTF8';";
}

sub add_multi {
    my ($self, $data, %opts) = @_;

    my @data = @$data;

    my $fields      = $self->_fields_hs();
    my $data_fields = array_uniq(map {keys(%$_)} @$data);
    my $field_names = arrays_intersection([map {$fields->{$_}->name} keys %$fields], $data_fields);

    my @locales = keys(%{$self->db->get_option('locales', {})});
    @locales = (undef) unless @locales;

    my $add_rows = 0;

    my $need_transact = @data > $ADD_CHUNK;
    $self->db->begin() if $need_transact;

    my $sql_header = ($opts{'replace'} ? 'REPLACE' : 'INSERT') . ' INTO ' . $self->quote_identifier($self->name) . ' (';
    my @real_field_names;
    foreach my $name (@$field_names) {
        if ($fields->{$name}{'i18n'}) {
            push(@real_field_names, defined($_) ? "${name}_${_}" : $name) foreach @locales;
        } else {
            push(@real_field_names, $name);
        }
    }
    $sql_header .= join(', ', map {$self->quote_identifier($_)} @real_field_names) . ") VALUES\n";

    while (my @add_data = splice(@data, 0, $ADD_CHUNK)) {
        my @params = ();
        my $sql    = $sql_header;
        foreach my $row (@add_data) {
            $sql .= ",\n" if $row != $add_data[0];
            $sql .= '(?' . ', ?' x (@real_field_names - 1) . ')';

            foreach my $name (@$field_names) {
                if ($fields->{$name}{'i18n'}) {
                    if (ref($row->{$name}) eq 'HASH') {
                        my @missed_langs = grep {!exists($row->{$name}{$_})} @locales;
                        throw Exception::BadArguments gettext('Undefined languages "%s" for field "%s"',
                            join(', ', @missed_langs), $name)
                          if @missed_langs;
                        push(@params, $row->{$name}{$_}) foreach @locales;
                    } elsif (!ref($row->{$name})) {
                        push(@params, $row->{$name}) foreach @locales;
                    } else {
                        throw Exception::BadArguments gettext('Invalid value in table->add');
                    }
                } else {
                    push(@params, $row->{$name});
                }
            }
        }

        $add_rows += $self->db->_do($sql, @params);
    }

    $self->db->commit() if $need_transact;

    return $add_rows;
}

sub add {
    my ($self, $data, %opts) = @_;

    $data = {$self->primary_key->[0] => $data} if !ref($data) && @{$self->primary_key || []} == 1;

    $self->add_multi([$data], %opts);

    my $fields_hs = $self->_fields_hs();
    my @res       = map {
        !defined($data->{$_})
          && $fields_hs->{$_}{'autoincrement'}
          ? $self->db->_get_all('SELECT LAST_INSERT_ID() AS `id`')->[0]{'id'}
          : $data->{$_}
    } @{$self->primary_key || []};

    return @res == 1 ? $res[0] : \@res;
}

sub edit {
    my ($self, $pkeys_or_filter, $data, %opts) = @_;

    my @fields = keys(%$data);

    my $sql = 'UPDATE ' . $self->quote_identifier($self->name) . "\n" . 'SET ';

    my $fields = $self->_fields_hs();

    my @locales = keys(%{$self->db->get_option('locales', {})});

    my $ssql             = '';
    my @real_field_names = ();
    my @field_data       = ();
    foreach my $name (@fields) {
        $ssql .= ",\n    " unless $ssql;
        if ($fields->{$name}{'i18n'} && @locales) {
            foreach my $locale (@locales) {
                push(@real_field_names, "${name}_${locale}");
                push(@field_data, ref($data->{$name}) eq 'HASH' ? $data->{$name}{$locale} : $data->{$name});
            }
        } else {
            push(@real_field_names, $name);
            push(@field_data,       $data->{$name});
        }
    }
    $sql .= join(",\n    ", map {$self->quote_identifier($_) . ' = ?'} @real_field_names) . "\n";

    my $query = $self->db->query()->select(table => $self, fields => {});
    my $filter_expr = $query->filter($self->_pkeys_or_filter_to_filter($pkeys_or_filter))->expression();
    my ($filter_sql, @filter_data) = $query->_field_to_sql(undef, $filter_expr, $query->_get_table($self));
    $sql .= 'WHERE ' . $filter_sql;

    return $self->db->_do($sql, @field_data, @filter_data);
}

sub delete {
    my ($self, $pkeys_or_filter, %opts) = @_;

    my $query = $self->db->query()->select(table => $self, fields => {});
    my $filter_expr = $query->filter($self->_pkeys_or_filter_to_filter($pkeys_or_filter))->expression();
    my ($filter_sql, @filter_data) = $query->_field_to_sql(undef, $filter_expr, $query->_get_table($self));

    $self->db->_do('DELETE FROM ' . $self->quote_identifier($self->name) . "\nWHERE $filter_sql", @filter_data);
}

sub replace {
    my ($self, $data, %opts) = @_;

    $self->add($data, %opts, replace => 1);
}

sub replace_multi {
    my ($self, $data, %opts) = @_;

    $self->add_multi($data, %opts, replace => 1);
}

sub _get_field_object {
    my ($self, %opts) = @_;

    return QBit::Application::Model::DB::mysql::Field->new(%opts);
}

sub _convert_fk_auto_type {
    my ($self, $field, $fk_field) = @_;

    $field->{$_} = $fk_field->{$_}
      foreach grep {exists($fk_field->{$_}) && !exists($field->{$_})} qw(type unsigned not_null length);
}

sub _create_sql_index {
    my ($self, $index) = @_;

    return
        ($index->{'unique'} ? 'UNIQUE ' : '') 
      . 'INDEX '
      . $self->quote_identifier(
        substr(join('_', ($index->{'unique'} ? 'uniq' : ()), $self->name, '', @{$index->{'fields'}}), 0, 64))
      . ' ('
      . join(', ', map {$self->quote_identifier($_)} @{$index->{'fields'}}) . ')';
}

sub _create_sql_foreign_key {
    my ($self, $key) = @_;

    return 'FOREIGN KEY '
      . $self->quote_identifier(
        substr(join('_', 'fk', $self->name, '', @{$key->[0]}, '_', $key->[1], '', @{$key->[2]}), 0, 64))
      . ' ('
      . join(', ', map {$self->quote_identifier($_)} @{$key->[0]}) . ")\n"
      . '        REFERENCES '
      . $self->quote_identifier($key->[1]) . ' ('
      . join(', ', map {$self->quote_identifier($_)} @{$key->[2]}) . ")\n"
      . "            ON UPDATE RESTRICT\n"
      . "            ON DELETE RESTRICT";
}

TRUE;
