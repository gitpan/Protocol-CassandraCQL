#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::Result;

use strict;
use warnings;
use base qw( Protocol::CassandraCQL::ColumnMeta );

our $VERSION = '0.07';

use Carp;

use Protocol::CassandraCQL qw( :types );

=head1 NAME

C<Protocol::CassandraCQL::Result> - stores the result of a Cassandra CQL query

=head1 DESCRIPTION

Objects in this class store the result of a direct query or executed prepared
statement, as returned by an C<OPCODE_RESULT> giving C<RESULT_ROWS>. It allows
convenient access to the decoded row data.

As a subclass of L<Protocol::CassandraCQL::ColumnMeta> it also provides
information about column metadata, such as column names and types.

=cut

=head1 CONSTRUCTORS

=head2 $result = Protocol::CassandraCQL::Result->from_frame( $frame )

Returns a new result object initialised from the given C<OPCODE_RESULT> /
C<RESULT_ROWS> message frame.

=cut

sub from_frame
{
   my $class = shift;
   my ( $frame ) = @_;
   my $self = $class->SUPER::from_frame( $frame );

   my $n_rows = $frame->unpack_int;
   my $n_columns = $self->columns;

   $self->{rows} = [];
   foreach ( 1 .. $n_rows ) {
      my @rowbytes = map { $frame->unpack_bytes } 1 .. $n_columns;
      push @{$self->{rows}}, [ $self->decode_data( @rowbytes ) ];
   }

   return $self;
}

=head2 $result = Protocol::CassandraCQL::Result->new( %args )

Returns a new result object initialised directly from the given row data. This
constructor is intended for use by unit test scripts, to create results
directly from mocked connection objects or similar.

In addition to the arguments taken by the superclass constructor, it takes the
following named arguments:

=over 8

=item rows => ARRAY[ARRAY]

An ARRAY reference containing ARRAY references of the individual rows' data.

=back

=cut

sub new
{
   my $class = shift;
   my %args = @_;

   my $rows = delete $args{rows};

   my $self = $class->SUPER::new( %args );

   $self->{rows} = \my @rows;

   foreach my $ri ( 0 .. $#$rows ) {
      my $row = $rows->[$ri];

      foreach my $ci ( 0 .. $self->columns-1 ) {
         my $e = $self->column_type( $ci )->validate( $row->[$ci] ) or next;
         croak "Cannot construct row $ri: ".$self->column_shortname( $ci ).": $e";
      }

      push @rows, [ @$row ];
   }

   return $self;
}

=head2 $n = $result->rows

Returns the number of rows

=cut

sub rows
{
   my $self = shift;
   return scalar @{ $self->{rows} };
}

=head2 $data = $result->row_array( $idx )

Returns the row's data decoded, as an ARRAY reference

=cut

sub row_array
{
   my $self = shift;
   my ( $idx ) = @_;

   croak "No such row $idx" unless $idx >= 0 and $idx < @{ $self->{rows} };

   # clone it so the caller can't corrupt our stored state
   return [ @{ $self->{rows}[$idx] } ];
}

=head2 $data = $result->row_hash( $idx )

Returns the row's data decoded, as a HASH reference mapping column short names
to values.

=cut

sub row_hash
{
   my $self = shift;
   my ( $idx ) = @_;

   croak "No such row $idx" unless $idx >= 0 and $idx < @{ $self->{rows} };

   return { map { $self->column_shortname( $_ ) => $self->{rows}[$idx][$_] } 0 .. $self->columns-1 };
}

=head2 @data = $result->rows_array

Returns a list of all the rows' data decoded as ARRAY references.

=cut

sub rows_array
{
   my $self = shift;
   return map { $self->row_array( $_ ) } 0 .. $self->rows-1;
}

=head2 @data = $result->rows_hash

Returns a list of all the rows' data decoded as HASH references.

=cut

sub rows_hash
{
   my $self = shift;
   return map { $self->row_hash( $_ ) } 0 .. $self->rows-1;
}

=head2 $map = $result->rowmap_array( $keyidx )

Returns a HASH reference mapping keys to rows deccoded as ARRAY references.
C<$keyidx> gives the column index of the value to use as the key in the
returned map.

=cut

sub rowmap_array
{
   my $self = shift;
   my ( $keyidx ) = @_;

   croak "No such column $keyidx" unless $keyidx >= 0 and $keyidx < $self->columns;

   return { map { $_->[$keyidx] => $_ } $self->rows_array };
}

=head2 $map = $result->rowmap_hash( $keyname )

Returns a HASH reference mapping keys to rows decoded as HASH references.
C<$keyname> gives the column shortname of the value to use as the key in the
returned map.

=cut

sub rowmap_hash
{
   my $self = shift;
   my ( $keyname ) = @_;

   croak "No such column '$keyname'" unless defined $self->find_column( $keyname );

   return { map { $_->{$keyname} => $_ } $self->rows_hash };
}

=head1 SPONSORS

This code was paid for by

=over 2

=item *

Perceptyx L<http://www.perceptyx.com/>

=item *

Shadowcat Systems L<http://www.shadow.cat>

=back

=head1 AUTHOR

Paul Evans <leonerd@leonerd.org.uk>

=cut

0x55AA;
