project('simpledb', 'c', version: '0.1.0')

add_project_arguments('-D_GNU_SOURCE', language: 'c')

subdir('src')

if meson.get_compiler('c').get_id() == 'gcc'
    add_project_arguments('-D_GNU_SOURCE', language: 'c')
endif

executable('simpledb', src_files)

subdir('test')
criterion = dependency('criterion')
t1 = executable('test_simpledb', test_files, dependencies: [criterion])
test('Database tests', t1)
