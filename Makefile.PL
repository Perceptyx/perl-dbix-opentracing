use ExtUtils::MakeMaker::CPANfile;

WriteMakefile(
    'NAME'          => 'DBIx::OpenTracing',
    'VERSION_FROM'  => 'lib/DBIx/OpenTracing.pm',
    'ABSTRACT_FROM' => 'lib/DBIx/OpenTracing.pod',
    'LICENSE'       => 'perl',
    test            => {
        TESTS       => 't/*.t',
    },
);
