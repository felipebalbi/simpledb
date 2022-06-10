project('simpledb', 'c', version: '0.1.0')

subdir('src')
executable('simpledb', src_files)

subdir('test')
criterion = dependency('criterion')
t1 = executable('test_simpledb', test_files, dependencies: [criterion])
test('Simple test', t1)
