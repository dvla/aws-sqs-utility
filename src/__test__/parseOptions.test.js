const parseOptions = require('../parseOptions');

const originalArgv = process.argv;
const originalExit = process.exit;

beforeEach(() => {
    //    global.console.error = jest.fn(global.console.error);
    global.console.error = jest.fn();
});

afterAll(() => {
    process.argv = originalArgv;
    process.exit = originalExit;
});

describe('parseOptions', () => {
    it('should error if invalid action specified', async () => {
        // Given
        process.argv = ['', '', '--xyz'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Unknown option: --xyz');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if no action specified', async () => {
        // Given
        process.argv = ['', ''];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: No action specified');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if multiple actions specified', async () => {
        // Given
        process.argv = ['', '', '-l', '--delete'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: Multiple actions specified [load, delete]');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if file and target specified', async () => {
        // Given
        process.argv = ['', '', '--file', 'myFile', '--target', 'myTarget'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: Cannot specify both file and target');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should parse valid action - queues', async () => {
        // Given
        process.argv = ['', '', '--queues'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.queues).toBe(true);
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should parse valid alias - queues', async () => {
        // Given
        process.argv = ['', '', '-q'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.queues).toBe(true);
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should error if file specified for queues action', async () => {
        // Given
        process.argv = ['', '', '--queues', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File/target not allowed for queues action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if target specified for queues action', async () => {
        // Given
        process.argv = ['', '', '-q', '-t', 'myTarget'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File/target not allowed for queues action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should parse valid action - describe', async () => {
        // Given
        process.argv = ['', '', '--describe', 'myQueue'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.describe).toBe('myQueue');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should error if file specified for describe action', async () => {
        // Given
        process.argv = ['', '', '--describe', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File/target not allowed for describe action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if target specified for descibe action', async () => {
        // Given
        process.argv = ['', '', '--describe', 'myQueue', '-t', 'myTarget'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File/target not allowed for describe action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should parse valid action - list + file', async () => {
        // Given
        process.argv = ['', '', '--list', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.list).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should parse valid alias - list + file', async () => {
        // Given
        process.argv = ['', '', '-i', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.list).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should error if no file/target specified for list action', async () => {
        // Given
        process.argv = ['', '', '--list', 'myQueue'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File/target required for list action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should parse valid action - extract + file', async () => {
        // Given
        process.argv = ['', '', '--extract', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.extract).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should parse valid alias - extract + file', async () => {
        // Given
        process.argv = ['', '', '-e', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.extract).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should error if no file/target specified for extract action', async () => {
        // Given
        process.argv = ['', '', '--extract', 'myQueue'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File/target required for extract action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should parse valid action - load + file', async () => {
        // Given
        process.argv = ['', '', '--load', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.load).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should parse valid alias - load + file', async () => {
        // Given
        process.argv = ['', '', '-l', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.load).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should error if no file specified for load action', async () => {
        // Given
        process.argv = ['', '', '--load', 'myQueue'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File required for load action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should parse valid action - delete + file', async () => {
        // Given
        process.argv = ['', '', '--delete', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.delete).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should parse valid alias - delete + file', async () => {
        // Given
        process.argv = ['', '', '-d', 'myQueue', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        const options = parseOptions();

        // Then
        expect(options.delete).toBe('myQueue');
        expect(options.file).toBe('myFile');
        expect(process.exit).not.toHaveBeenCalled();
    });

    it('should error if no file specified for delete action', async () => {
        // Given
        process.argv = ['', '', '--delete', 'myQueue'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: File required for delete action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if no queue specified for delete action', async () => {
        // Given
        process.argv = ['', '', '--delete', '-f', 'myFile'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: No queue specified for delete action');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if limit is not a number', async () => {
        // Given
        process.argv = ['', '', '--list', 'myQueue', '--limit', 'abc'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: Invalid limit');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    it('should error if limit is not positive', async () => {
        // Given
        process.argv = ['', '', '--list', 'myQueue', '--limit', '-1'];
        process.exit = jest.fn();

        // When
        parseOptions();

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error: Invalid limit');
        expect(process.exit).toHaveBeenCalledWith(1);
    });
});
