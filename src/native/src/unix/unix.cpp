#include "unix/unix.h"

#include <clocale>
#include <spdlog/spdlog.h>

namespace dcn::native {
    bool configureTerminal()
    {
        const char * locale_name = ::setlocale(LC_ALL, "");
        if (locale_name == nullptr)
        {
            return false;
        }

        return true;
    }

    std::pair<int, std::string> runProcess(const std::string& command, std::vector<std::string> args)
    {
        const bool has_path_separator = command.find('/') != std::string::npos;
        if (has_path_separator && access(command.c_str(), X_OK) != 0)
        {
            throw std::runtime_error("Command not executable: " + command);
        }

        // Build argv array
        std::vector<char*> argv;
        argv.reserve(args.size() + 2); // command + args + null
        argv.push_back(const_cast<char*>(command.c_str())); // argv[0] = program name

        for (const auto& arg : args)
        {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);

        int pipefd[2];
        if (pipe(pipefd) == -1)
        {
            spdlog::error("Failed to create pipe");
            throw std::runtime_error("Failed to create pipe");
        }

        pid_t pid = fork();
        if (pid < 0)
        {
            spdlog::error("Failed to fork");
            throw std::runtime_error("Failed to fork");
        }

        if (pid == 0) 
        {
            // Child process
            close(pipefd[0]); // Close read end
            dup2(pipefd[1], STDOUT_FILENO);
            dup2(pipefd[1], STDERR_FILENO);
            close(pipefd[1]);

            execvp(command.c_str(), argv.data());

            // If exec fails
            spdlog::error("execvp failed");
            perror("execvp failed");
            _exit(127);
        }

        // Parent process
        close(pipefd[1]); // Close write end

        std::string output;
        char buffer[256];
        ssize_t count;

        while ((count = read(pipefd[0], buffer, sizeof(buffer))) > 0)
            output.append(buffer, count);

        close(pipefd[0]);

        int status;
        if (waitpid(pid, &status, 0) == -1)
        {
            spdlog::error("waitpid failed");
            throw std::runtime_error("waitpid failed");
        }

        int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;

        return {exit_code, output};
    }
} // namespace dcn::native
