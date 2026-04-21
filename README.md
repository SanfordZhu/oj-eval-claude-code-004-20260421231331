# Bookstore Management System

**ACMOJ Problem ID**: 1075 (Main Test), 1775 (Hidden Test)

## Table of Contents

- [Bookstore Management System](#bookstore-management-system)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
    - [Background](#background)
  - [Assignment Description](#assignment-description)
    - [Grade Composition](#grade-composition)
  - [Assignment Requirements](#assignment-requirements)
    - [Basic Requirements](#basic-requirements)
    - [Interaction Method](#interaction-method)
    - [Account System](#account-system)
      - [Privilege Levels](#privilege-levels)
      - [Login Stack](#login-stack)
      - [Account Information](#account-information)
      - [Account-Related Commands](#account-related-commands)
    - [Book System](#book-system)
      - [Book Information](#book-information)
      - [Book-Related Commands](#book-related-commands)
    - [Log System](#log-system)
      - [Log Information](#log-information)
      - [Log-Related Commands](#log-related-commands)
  - [Per-Testcase Resource Limits](#per-testcase-resource-limits)
  - [Performance Requirements](#performance-requirements)
  - [Submission Requirements](#submission-requirements)
    - [OJ Git Repository Compilation Process](#oj-git-repository-compilation-process)
    - [Git Configuration Requirements](#git-configuration-requirements)
    - [Submission Guidelines](#submission-guidelines)
    - [Evaluation Notes](#evaluation-notes)
    - [Academic Integrity](#academic-integrity)

## Introduction

### Background

The Bookstore Management System is a comprehensive management platform serving **store owners (super administrators)**, **employees (administrators)**, and **customers (users)**. The system manages account privileges, book information, inventory data, and financial records, providing corresponding functional services for users with different privilege levels. The system emphasizes data persistence and efficient file read/write operations.

## Assignment Description

### Grade Composition

| Grading Component | Percentage |
| :--: | :--: |
| Pass **1075. Bookstore Management System (Main Test)** | 60% |
| Pass **1775. Bookstore Management System (Hidden Test)** | 20% |
| Code Review | 20% |

Here are several points that need clarification:

- In the Code Review, we will **strictly examine your code style and repository organization structure, etc.**. 

- This assignment provides test data for **1075** stored in the `/workspace/data/004/1075/` directory. These are all the test cases for problem 1075. Note that **1775 test cases are not publicly available**.

- Besides the provided test data, we also encourage you to design your own test data based on your program logic to assist debugging.

- It is recommended to start implementing simple functions and gradually improve the system, ensuring each module is thoroughly tested.

- The program must automatically create a super administrator account with username `root`, password `sjtu`, and privilege level {7} on first run.

## Assignment Requirements

### Basic Requirements

Design and implement a bookstore management system that meets the following basic requirements:

- **Programming Language Restriction**: The software main body (excluding bonus parts) must be written entirely in C++
- **Data Persistence**: Program data must be recorded to files
- **Automatic Initialization**: The program autonomously executes required initialization operations on first run
  - Create a super administrator account with username `root`, password `sjtu`, and privilege level {7}
- **Execution Mode**: After program startup, execute corresponding operations based on input data until EOF is read or the program terminates according to relevant command requirements

### Interaction Method

Use command line (Windows CMD or Linux Terminal) for operations. Input data is separated by newline or carriage return characters into multiple commands.

**Character Set and Whitespace Rules:**
- Legal character set for commands is standard ASCII characters
- Only space is allowed as whitespace; a single command is split into multiple parts by spaces
- Multiple consecutive spaces are equivalent to a single space
- Extra spaces are allowed at the beginning and end of lines
- Unless otherwise specified, spaces cannot be omitted or added arbitrarily

**Command Format Rules:**
- The first part of a command must be a command keyword
- Keyword parts in commands must match the command format exactly

**Grammar Notation** (weakened regex syntax):
- `[x]` represents a user-defined string with specific restrictions
- `(a|b|c)` means only one of `a`, `b`, `c` can appear here
- `(x)?` means `x` can appear zero or once here
- `(x)+` means `x` can appear one or more times here

**Legal Command List:**

```
# Basic Commands
quit
exit

# Account System Commands
su [UserID] ([Password])?
logout
register [UserID] [Password] [Username]
passwd [UserID] ([CurrentPassword])? [NewPassword]
useradd [UserID] [Password] [Privilege] [Username]
delete [UserID]

# Book System Commands
show (-ISBN=[ISBN] | -name="[BookName]" | -author="[Author]" | -keyword="[Keyword]")?
buy [ISBN] [Quantity]
select [ISBN]
modify (-ISBN=[ISBN] | -name="[BookName]" | -author="[Author]" | -keyword="[Keyword]" | -price=[Price])+
import [Quantity] [TotalCost]

# Log System Commands
show finance ([Count])?
log
report finance
report employee
```

**Input/Output Rules:**
- After a user inputs a command, if it is legal, execute the corresponding operation and output the result if any
- If the command is illegal or the operation fails, output `Invalid\n`
- Commands containing only spaces are legal and produce no output
- Unless otherwise specified, if the output content of an input command is non-empty, it ends with a `\n` character; if the output content is empty, no characters are output
- The `quit` and `exit` commands function to normally terminate the system

### Account System

The bookstore management system serves **store owners (super administrators)**, **employees (administrators)**, and **customers (users)**. To meet their different needs, an account system is required to manage account and privilege data and provide corresponding functions.

#### Privilege Levels

In this system, {x} represents privilege level, where larger numbers indicate higher privileges. Account privilege levels are {7}, {3}, and {1}; when no account is logged in, the current account privilege is considered {0}.

**Privilege Inheritance Rules:**
- Functions accessible by low-privilege accounts are strictly a subset of functions accessible by high-privilege accounts
- A command's privilege level indicates the minimum current account privilege required to execute that command
- High-privilege accounts can execute commands requiring low privileges, but not vice versa

**Functions Accessible by Each Privilege Level:**
- **Store Owner {7}**: Uses the `root` account, can access all bookstore system functions, including logs and account management functions
- **Employee {3}**: Can access functions related to modifying book data, etc.
- **Customer {1}**: Can register accounts, query and purchase books
- **Guest {0}**: Can register accounts

**IMPORTANT**: There are only the above four privilege levels in this system; other privilege levels are illegal.

#### Login Stack

The system supports nested login, meaning multiple accounts can be in a logged-in state simultaneously; the same account can be logged in multiple times simultaneously. Input commands are considered operations by the last logged-in account. Exiting the system logs out all logged-in accounts.

**Login Stack Example:** (Assume all users exist and all passwords are correct)

```bash
# Initially, the login stack is empty
su root sjtu
select first
# Login stack is now: root, selected book is first

su root sjtu
# Login stack is now: root root, no selected book
select second

su zhang beihai
select third
# Login stack is now: root root zhang, selected book is third
# Operating user is zhang, cannot execute delete and other commands

su chu yan
# Login stack is now: root root zhang chu, operating user is chu, no selected book

logout
# Login stack is now: root root zhang, selected book is third

logout
# Login stack is now: root root, selected book is second

logout
# Login stack is now: root, selected book is first
```

#### Account Information

User-defined strings in command formats are restricted as follows, where "maximum length" means the storage space occupied by the string cannot exceed the storage space occupied by the specified number of English letter characters.

- `[UserID]`, `[Password]`, `[CurrentPassword]`, `[NewPassword]`
  - Legal character set: digits, letters, underscores
  - Maximum length: 30
- `[Username]`
  - Legal character set: ASCII characters except invisible characters
  - Maximum length: 30
- `[Privilege]`
  - Legal character set: digits
  - Maximum length: 1

**IMPORTANT**: Commands containing user-defined strings that do not comply with the above restrictions are considered illegal.

#### Account-Related Commands

The command description format in this document is as follows:

- **Command Name**
  - {Command Privilege}  `[Command Format]`
  - Command function
  - Command logic (variable number of items, priority order same as listing order)

The following are account system command descriptions:

- **Login Account**
  - {0} `su [UserID] ([Password])?`
  - Changes the login account to logged-in state, and the current account becomes this account
    - If the account does not exist, the operation fails
    - If the password is incorrect, the operation fails
    - If the current account's privilege level is higher than the login account's, the password can be omitted

- **Logout Account**
  - {1} `logout`
  - Revokes the effect of the last successfully executed `su` command
    - If no account is logged in, the operation fails

- **Register Account**
  - {0} `register [UserID] [Password] [Username]`
  - Registers an account with information as shown in the command format, with privilege level {1}
    - If `[UserID]` duplicates an already registered account, the operation fails

- **Change Password**
  - {1} `passwd [UserID] ([CurrentPassword])? [NewPassword]`
  - Changes the password of the specified account
    - If the account does not exist, the operation fails
    - If the password is incorrect, the operation fails
    - If the current account's privilege level is {7}, `[CurrentPassword]` can be omitted

- **Create Account**
  - {3} `useradd [UserID] [Password] [Privilege] [Username]`
  - Creates an account with information as shown in the command format
    - If the privilege level of the account to be created is greater than or equal to the current account's privilege level, the operation fails
    - If `[UserID]` duplicates an already registered account, the operation fails

- **Delete Account**
  - {7} `delete [UserID]`
  - Deletes the specified account
    - If the account to be deleted does not exist, the operation fails
    - If the account to be deleted is logged in, the operation fails

### Book System

The book system provides services and data storage functions related to book information, inventory information, and book transaction financial records.

#### Book Information

- `[ISBN]`: Internal Strange Book Number
  - Legal character set: ASCII characters except invisible characters
  - Maximum length: 20
  - **Special Note**: In this system, no two books should have the same `[ISBN]` information

- `[BookName]`, `[Author]`: Book name, author name
  - Legal character set: ASCII characters except invisible characters and English double quotes
  - Maximum length: 60

- `[Keyword]`: Book keywords
  - Legal character set: ASCII characters except invisible characters and English double quotes
  - Maximum length: 60
  - **Special Note**: `[Keyword]` content can contain multiple segments separated by `|`. For example, `math|magic|quantum` indicates the book has three unordered keywords `math`, `magic`, and `quantum`, with each segment having a length of at least 1

- `[Quantity]`: Purchase quantity
  - Legal character set: digits
  - Maximum length: 10
  - Special note: Value does not exceed 2,147,483,647

- `[Price]`, `[TotalCost]`: Book unit price, transaction total amount
  - Legal character set: digits and `.`
  - Maximum length: 13
  - Special note: Floating-point numbers in this system have fixed input/output precision of two decimal places

**Default Value Rules:**
- Character-related information defaults to empty
- Number-related information defaults to numeric value 0

#### Book-Related Commands

- **Search Books**
  - {1} `show (-ISBN=[ISBN] | -name="[BookName]" | -author="[Author]" | -keyword="[Keyword]")?`
  - Output book information in ascending lexicographic order by `[ISBN]`. Each book's information is output in the format `[ISBN]\t[BookName]\t[Author]\t[Keyword]\t[Price]\t[StockQuantity]\n`, where keywords in `[Keyword]` are in the input order
    - Output empty line when no books meet requirements
    - When no additional parameters are provided, all books meet requirements
    - Operation fails if additional parameter content is empty
    - Operation fails if `[Keyword]` contains multiple keywords

- **Buy Books**
  - {1} `buy [ISBN] [Quantity]`
  - Purchase the specified quantity of the specified book (since this system does not include a payment system, only reduce inventory), output the total amount required to purchase the books as a floating-point number
    - Operation fails if no matching book exists
    - Operation fails if purchase quantity is not a positive integer
    - Operation fails if inventory is insufficient

- **Select Book**
  - {3} `select [ISBN]`
  - The **current account** selects the specified book (no need to save selected book information after account logout)
    - If no matching book exists, create a new book with only `[ISBN]` information
    - Exiting the system cancels book selection

- **Modify Book Information**
  - {3} `modify (-ISBN=[ISBN] | -name="[BookName]" | -author="[Author]" | -keyword="[Keyword]" | -price=[Price])+`
  - Update the selected book's information with the information in the command
    - Operation fails if no book is selected
    - Duplicate additional parameters make the command illegal
    - Operation fails if additional parameter content is empty
    - Cannot change ISBN to its original ISBN, for example:
      ```
      select example
      modify -ISBN=example
      ```
      is an illegal operation
    - Cannot change ISBN to another ISBN already existing in the system
    - Operation fails if `[Keyword]` contains duplicate segments

- **Import Books**
  - {3} `import [Quantity] [TotalCost]`
  - Purchase the specified quantity of the selected book at the specified transaction total amount, increasing its inventory
    - Operation fails if no book is selected
    - Operation fails if purchase quantity is not a positive integer
    - Operation fails if transaction total amount is not a positive number

### Log System

The log system is responsible for recording various data during the operation of the bookstore management system and providing various log information query services.

#### Log Information

- `[Count]`: Number of transactions
  - Legal character set: digits
  - Maximum length: 10
  - Special note: Value does not exceed 2,147,483,647

#### Log-Related Commands

- **Financial Records Query**
  - {7} `show finance ([Count])?`
  - Output the total amount of the last specified number of completed transactions in the format `+ [Income] - [Expenditure]\n`, for example `+ 1.23 - 45678.90`
    - Without `Count` parameter, output total amount of all transactions
      - When no transactions exist, consider both income and expenditure as 0.00
    - Output empty line when `Count` is 0
    - Operation fails when `Count` is greater than the total number of historical transactions

- **Generate Financial Records Report**
  - {7} `report finance`
  - Generate a pleasing financial report with self-defined format

- **Generate Employee Work Report**
  - {7} `report employee`
  - Generate a pleasing employee work report recording their operations, with self-defined format

- **Generate Log**
  - {7} `log`
  - Return log records, including system operations (who did what) and financial transaction details for each transaction, with self-defined format

## Per-Testcase Resource Limits

### 1075 - Bookstore Management System (Main Test)

- **Time Limit (per test case)**: 10000 ms  
- **Memory Limit (per test case)**: 64 MiB  
- **Disk Usage Limit (per test case)**: 1024 MiB  
- **File Count Limit**: 20  

### 1775 - Bookstore Management System (Hidden Test)

- **Time Limit (per test case)**: 10000 ms  
- **Memory Limit (per test case)**: 64 MiB  
- **Disk Usage Limit (per test case)**: 1024 MiB  
- **File Count Limit**: 20 

## Performance Requirements

Considering the large number of accounts and books in the application scenario, **it is prohibited to store main data in memory**. File data should be read and written in real-time.

**Performance Constraints:**
- The number of files created by the program during operation must not exceed 20
- Implementation methods similar to filesystem as database are prohibited

**Estimated Data Scale:**
- Number of accounts: May reach tens of thousands
- Number of books: May reach tens of thousands
- Number of operations: May reach hundreds of thousands

## Submission Requirements

### OJ Git Repository Compilation Process

For Git compilation, we will first clone the repository using a command similar to:
```bash
git clone <repo_url> . --depth 1 --recurse-submodules --shallow-submodules --no-local
```

Then we check if there is a `CMakeLists.txt` file. If it exists, we run (if not, a warning message will be displayed):
```bash
cmake .
```

Finally, we check if there is any of `GNUmakefile`/`makefile`/`Makefile` (if cmake was run previously, this will be the generated Makefile). If it exists, we run (if not, a warning message will be displayed):
```bash
make
```

After this process is complete, we will use the `code` file in the project root directory as the compilation result.

The project does not provide a CMakeLists.txt file, so you need to create and edit it yourself. The local environment has gcc-13 and g++-13 available.

### Git Configuration Requirements

**IMPORTANT**: You must create a `.gitignore` file in your project root directory to avoid OJ evaluation conflicts.

The `.gitignore` file should include at least the following entries:

```gitignore
CMakeFiles/
CMakeCache.txt
```

### Submission Guidelines

- The submitted code must be able to compile successfully through the above compilation process
- The compiled executable file name must be `code`
- The program needs to be able to read data from standard input and write results to standard output
- Please ensure the code runs correctly within the given time and space limits
- **You must use C++** to implement this assignment

### Evaluation Notes

- The evaluation system will test your program using the provided test data for 1075 and hidden test data for 1775
- The program output must exactly match the expected output (including format)
- Exceeding time or memory limits will be judged as the corresponding error type
- Passing all 1075 test cases does not guarantee that you will pass the 1775 hidden tests
- It is recommended to design additional test cases to cover edge cases and complex scenarios
- Please pay attention to the overall time performance of your code and the time complexity of each part of your algorithm.

### Academic Integrity

If any violations are found during evaluation or code review (including but not limited to using unconventional methods to pass test cases), your final score may be significantly reduced or become **0 points**.