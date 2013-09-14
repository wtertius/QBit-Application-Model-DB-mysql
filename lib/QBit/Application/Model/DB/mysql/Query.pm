package QBit::Application::Model::DB::mysql::Query;

use qbit;

use base qw(QBit::Application::Model::DB::Query);

sub with_rollup {
    my ($self, $value) = @_;

    $self->{'__WITH_ROLLUP__'} = !!$value;
}

sub _after_select {
    my ($self) = @_;

    return $self->{'__CALC_ROWS__'} ? ' SQL_CALC_FOUND_ROWS' : '';
}

sub _after_group_by {
    my ($self) = @_;

    return $self->{'__WITH_ROLLUP__'} ? ' WITH ROLLUP' : '';
}

sub _found_rows {
    my ($self) = @_;

    return $self->db->_get_all('SELECT FOUND_ROWS() AS `rows`')->[0]{'rows'};
}

TRUE;
