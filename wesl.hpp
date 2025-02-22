#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <variant>
#include <iostream>
#include <string_view>
#include <map>
#include <sstream>
#include <deque>
#include <cassert>

template <typename I, typename F, typename S, typename U>
class WESL {
public:
	typedef uint32_t Size;

	typedef int Type;

	static inline const Type Any = 0;
	static inline const Type Bool = 1;
	static inline const Type Integer = 2;
	static inline const Type Float = 3;
	static inline const Type String = 4;
	static inline const Type UserData = 5;

	// Span types are value type codes scaled up
	static inline const Type SpanTypes = 100;

	static std::string typeName(Type t) {
		switch (t) {
			case Any: return "Any";
			case Bool: return "Bool";
			case Integer: return "Integer";
			case Float: return "Float";
			case String: return "String";
			case UserData: return "UserData";
		}
		return t > SpanTypes ? "Span[" + typeName(t/SpanTypes) + "]": "(bad type code)";
	}

	static Type toSpanType(Type t) {
		assert(t < SpanTypes);
		return t*SpanTypes;
	}

	static Type fromSpanType(Type t) {
		assert(t > SpanTypes);
		return t/SpanTypes;
	}

	// Runtime decayed function. See FunctionDefinition{}
	struct Function {
		Size entry = 0; // offset into byte code vector
		Size frame = 0; // size of stack frame: locals + array cells
		Size args = 0;  // number of leading argument locals
	};

	// Runtime decayed span. See compileSpan() calls
	struct Span {
		Size start = 0;
		Size width = 0;
		bool operator==(const Span& o) const { return start == o.start && width == o.width; }
		bool operator!=(const Span& o) const { return start != o.start || width != o.width; }
		bool operator<(const Span& o) const { return start < o.start; }
		bool operator<=(const Span& o) const { return start <= o.start; }
		bool operator>(const Span& o) const { return start > o.start; }
		bool operator>=(const Span& o) const { return start >= o.start; }
	};

	// Runtime stack holds values
	typedef std::variant<bool,I,F,S,U,Span> Value;

	friend std::ostream& operator<<(std::ostream& os, const Value& v) {
		struct Printer {
			std::ostream& os;
			Printer(std::ostream& o) : os{o} {}
			void operator() (const bool& v) { os << std::string(v ? "true": "false"); }
			void operator() (const I& v) { os << v; }
			void operator() (const F& v) { os << v; }
			void operator() (const S& v) { os << v; }
			void operator() (const U& v) { os << "(userdata)"; }
			void operator() (const Span& v) { os << "(span:" + std::to_string(v.start) + ":" + std::to_string(v.width) + ")"; }
		};
		std::visit(Printer(os), v);
		return os;
	}

	// External code calling an internal function
	struct CallIn {
		Size entry = 0;
		std::vector<Type> args;
		Size frame = 0;
	};

	// Internal code calling an external function
	struct CallOut {
		Type type;
		std::vector<Type> args;
		std::function<Value(WESL&,std::span<Value>)> fn;
	};

private:

	// A persistent stack frame holding globals. Also see globalFunction
	Size globalFrame = 0;

	std::map<std::string,CallIn> callins;
	std::map<std::string,CallOut> callouts;

	struct Token {
		enum {
			None,
			Symbol,
			StringLit,
			IntLit,
			FloatLit,
			Punct,
		} type = None;
		std::string_view str;

		explicit operator bool() const { return type != None; }

		friend std::ostream& operator<<(std::ostream& os, const Token& t) {
			os << [&]() {
				switch (t.type) {
					case Token::None: return "None";
					case Token::Symbol: return "Symbol";
					case Token::StringLit: return "StringLit";
					case Token::IntLit: return "IntLit";
					case Token::FloatLit: return "FloatLit";
					case Token::Punct: return "Punct";
				}
				throw;
			}() << ',' << t.str;
			return os;
		}
	};

	static std::string tokenDump(const Token& t) {
		switch (t.type) {
			case Token::None: return "None:" + std::string(t.str);
			case Token::Symbol: return "Symbol:" + std::string(t.str);
			case Token::StringLit: return "StringLit:" + std::string(t.str);
			case Token::IntLit: return "IntLit:" + std::string(t.str);
			case Token::FloatLit: return "FloatLit:" + std::string(t.str);
			case Token::Punct: return "Punct:" + std::string(t.str);
		}
		throw;
	}

	std::vector<Token> tokeniseSourceCode(std::string_view src) {
		using namespace std;

		vector<Token> tokens;

		auto peek = [&]() -> char {
			return src.size() ? src.front(): char(0);
		};

		auto peek2 = [&]() -> char {
			return src.size() > 1 ? src[1]: char(0);
		};

		auto take = [&]() -> char {
			if (src.size()) {
				char c = src.front();
				src.remove_prefix(1);
				return c;
			}
			return char(0);
		};

		auto iswhite = [&]() {
			return isspace(peek());
		};

		auto iscomment = [&]() {
			return (peek() == '/' && peek2() == '/');
		};

		auto skipwhite = [&]() {
			while (iswhite()) take();
		};

		auto skipcomment = [&]() {
			while (iscomment()) {
				take();
				take();
				while (peek() && peek() != '\n') take();
			}
		};

		auto skipstuff = [&]() {
			while (iswhite() || iscomment()) {
				skipwhite();
				skipcomment();
			}
		};

		auto token = [&]() -> Token {
			skipstuff();
			if (!peek()) return Token();
			const char* start = src.data();
			if (isalpha(peek())) {
				while (isalnum(peek()) || peek() == '_') take();
				return {Token::Symbol,string_view(start,src.data()-start)};
			}
			if (peek() == '"') {
				take();
				while (peek() && peek() != '"') {
					if (peek() == '\\') take();
					take();
				}
				if (peek() == '"') take();
				return {Token::StringLit,string_view(start,src.data()-start)};
			}
			if (isdigit(peek())) {
				bool flit = false;
				while (isdigit(peek()) || peek() == '.') {
					flit = flit || peek() == '.';
					take();
				}
				return {flit ? Token::FloatLit: Token::IntLit, string_view(start,src.data()-start)};
			}
			take();
			return {Token::Punct,string_view(start,1)};
		};

		while (src.size()) tokens.push_back(token());
		if (tokens.size() && !tokens.back()) tokens.pop_back();

		return tokens;
	}

	static bool isType(const Token& t) {
		return t.type == Token::Symbol && (
			t.str == "void" ||
			t.str == "int" ||
			t.str == "bool" ||
			t.str == "float" ||
			t.str == "string" ||
			t.str == "udata"
		);
	}

	static Type tokenType(const Token& t) {
		if (t.str == "void") return Any;
		if (t.str == "int") return Integer;
		if (t.str == "bool") return Bool;
		if (t.str == "float") return Float;
		if (t.str == "string") return String;
		if (t.str == "udata") return UserData;
		throw;
	}

	static bool isName(const Token& t) {
		return t.type == Token::Symbol && !isType(t);
	}

	static bool isPunct(const Token& t, char c) {
		return t.type == Token::Punct && c == t.str.front();
	}

	static bool isOpeningParen(const Token& t) {
		return isPunct(t, '(');
	}

	static bool isClosingParen(const Token& t) {
		return isPunct(t, ')');
	}

	static bool isOpeningBrace(const Token& t) {
		return isPunct(t, '{');
	}

	static bool isClosingBrace(const Token& t) {
		return isPunct(t, '}');
	}

	static bool isOpeningSquareBracket(const Token& t) {
		return isPunct(t, '[');
	}

	static bool isClosingSquareBracket(const Token& t) {
		return isPunct(t, ']');
	}

	static bool isComma(const Token& t) {
		return isPunct(t, ',');
	}

	static bool isAssignment(const Token& t) {
		return isPunct(t, '=');
	}

	static bool isSemiColon(const Token& t) {
		return isPunct(t, ';');
	}

	enum class Opcode {
		Print,
		AddI,
		AddF,
		AddS,
		SubI,
		SubF,
		MulI,
		MulF,
		DivI,
		DivF,
		ModI,
		Lt,
		Gt,
		Lte,
		Gte,
		Eq,
		NEq,
		Literal,
		Drop,
		Jump,
		JZ,
		LStore,
		LFetch,
		GStore,
		GFetch,
		Call,
		Return,
		CallOut,
		LSpan,
		GSpan,
		Get,
		Set,
		ICast,
		FCast,
		BCast,
	};

	// "byte code"
	std::vector<std::variant<Opcode,Size,bool,I,S,F,Function,CallOut*>> code;

	struct VariableDefinition {
		std::string name;
		Type type;
		Size offset = 0; // into a stack frame
		Size width = 0; // used by arrays
	};

	struct BlockDefinition {
		std::map<std::string,VariableDefinition> variables;
		std::vector<Size> cjumps; // resolve continue branches
		std::vector<Size> bjumps; // resolve break branches
	};

	struct FunctionDefinition {
		std::string name;
		Type type;
		Size entry = 0; // offset into byte code vector
		Size frame = 0; // number of locals + array cells
		std::vector<Type> args;
		std::deque<BlockDefinition> blocks;
	};

	void processSourceCode(std::string_view src) {
		using namespace std;

		auto tokens = tokeniseSourceCode(src);
		span<const Token> cursor = tokens;

		// Statements and expression boundaries update 'here'
		auto here = cursor.front();

		// Error message prefix for a token. Usually where(here)
		auto where = [&](const Token& t) {
			int line = 1;
			const char* p = src.data();
			while (p < t.str.data()) {
				if (*p++ == '\n') line++;
			}
			return "line " + std::to_string(line);
		};

		// There is no reflection; most compile-time info is discarded
		// FunctionDefinition decays to Function{}
		// SpanTypes decay to Span{}
		std::map<std::string,FunctionDefinition> functions;
		std::map<std::string,Size> enums;

		// Global "scope" is a top-level nameless function+block with
		// a stack frame that persists across ::execute() invocations
		auto globalFunction = &functions[""];
		FunctionDefinition* currentFunc = globalFunction;

		// Block scope
		vector<BlockDefinition*> blocks;

		// recursive descent
		std::function<Type(Type)> takeExpression;
		std::function<void(void)> takeStatement;

		auto beginBlock = [&]() {
			assert(currentFunc);
			currentFunc->blocks.emplace_back();
			blocks.push_back(&currentFunc->blocks.back());
		};

		auto endBlock = [&](Size cjump, Size bjump) {
			assert(blocks.size() > 1);
			for (auto mark: currentFunc->blocks.back().cjumps) code[mark] = cjump;
			for (auto mark: currentFunc->blocks.back().bjumps) code[mark] = bjump;
			currentFunc->blocks.pop_back();
			blocks.pop_back();
		};

		auto currentBlock = [&]() -> BlockDefinition& {
			assert(blocks.size());
			return *blocks.back();
		};

		auto peekName = [&]() {
			return cursor.size() > 0 && isName(cursor[0]);
		};

		auto peekFunctionDefinition = [&]() {
			return cursor.size() >= 3
				&& isType(cursor[0])
				&& isName(cursor[1])
				&& isOpeningParen(cursor[2])
			;
		};

		auto peekVariableDefinition = [&]() {
			return cursor.size() >= 3
				&& isType(cursor[0])
				&& isName(cursor[1])
				&& (isAssignment(cursor[2]) || isSemiColon(cursor[2]))
			;
		};

		auto peekArrayDefinition = [&]() {
			return cursor.size() >= 3
				&& isType(cursor[0])
				&& isName(cursor[1])
				&& isPunct(cursor[2],'[');
			;
		};

		auto peekVariableMutation = [&]() {
			return cursor.size() >= 2
				&& isName(cursor[0])
				&& isAssignment(cursor[1])
			;
		};

		auto peekVariableMutationAssign = [&]() {
			return cursor.size() >= 3
				&& isName(cursor[0])
				&& (isPunct(cursor[1],'+') || isPunct(cursor[1],'-') || isPunct(cursor[1],'*') || isPunct(cursor[1],'/'))
				&& isAssignment(cursor[2])
			;
		};

		auto peekSpanMutation = [&]() {
			return cursor.size() >= 2
				&& isName(cursor[0])
				&& isPunct(cursor[1],'[')
			;
		};

		auto peekFunctionCall = [&]() {
			return cursor.size() >= 2
				&& isName(cursor[0])
				&& isPunct(cursor[1],'(')
			;
		};

		auto peekWord = [&](const string& word) {
			return cursor.size() > 0 && cursor[0].str == word;
		};

		auto peekReturn = [&]() {
			return peekWord("return");
		};

		auto peekPrint = [&]() {
			return peekWord("print");
		};

		auto peekEnumDefinition = [&]() {
			return peekWord("enum");
		};

		auto peekBool = [&]() {
			return peekWord("true") || peekWord("false");
		};

		auto peekIf = [&]() {
			return peekWord("if");
		};

		auto peekElse = [&]() {
			return peekWord("else");
		};

		auto peekFor = [&]() {
			return peekWord("for");
		};

		auto peekWhile = [&]() {
			return peekWord("while");
		};

		auto peekContinue = [&]() {
			return peekWord("continue");
		};

		auto peekBreak = [&]() {
			return peekWord("break");
		};

		auto peekEnumItem = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::Symbol && enums.find(string(cursor[0].str)) != enums.end();
		};

		auto peekIntegerLiteral = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::IntLit;
		};

		auto peekFloatLiteral = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::FloatLit;
		};

		auto peekStringLiteral = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::StringLit;
		};

		auto peekCast = [&]() {
			return cursor.size() > 1 && isType(cursor[0]) && isPunct(cursor[1],'(');
		};

		auto peek = [&]() -> Token {
			return cursor.size() ? cursor.front(): Token();
		};

		auto take = [&]() -> Token {
			if (cursor.size()) {
				Token t = cursor.front();
				cursor = cursor.subspan(1);
				return t;
			}
			return Token();
		};

		auto takeType = [&]() {
			if (!isType(peek()))
				throw runtime_error(where(here) + ": expected type: " + tokenDump(peek()));
			return tokenType(take());
		};

		auto takeOpeningParen = [&]() {
			if (!isOpeningParen(peek()))
				throw runtime_error(where(here) + ": expected opening paren: " + tokenDump(peek()));
			take();
		};

		auto takeClosingParen = [&]() {
			if (!isClosingParen(peek()))
				throw runtime_error(where(here) + ": expected closing paren: " + tokenDump(peek()));
			take();
		};

		auto takeOpeningBrace = [&]() {
			if (!isOpeningBrace(peek()))
				throw runtime_error(where(here) + ": expected opening brace: " + tokenDump(peek()));
			take();
		};

		auto takeClosingBrace = [&]() {
			if (!isClosingBrace(peek()))
				throw runtime_error(where(here) + ": expected closing brace: " + tokenDump(peek()));
			take();
		};

		auto takeOpeningSquareBracket = [&]() {
			if (!isOpeningSquareBracket(peek()))
				throw runtime_error(where(here) + ": expected opening square bracket: " + tokenDump(peek()));
			take();
		};

		auto takeClosingSquareBracket = [&]() {
			if (!isClosingSquareBracket(peek()))
				throw runtime_error(where(here) + ": expected closing square bracket: " + tokenDump(peek()));
			take();
		};

		auto takeAssignment = [&]() {
			if (!isPunct(peek(),'='))
				throw runtime_error(where(here) + ": expected assignment operator: " + tokenDump(peek()));
			take();
		};

		auto takeSemiColon = [&]() {
			if (!isPunct(peek(),';'))
				throw runtime_error(where(here) + ": expected semi-colon: " + tokenDump(peek()));
			take();
		};

		auto takeStaticOffset = [&]() -> Size {
			if (peekIntegerLiteral())
				return Size(max(0,stoi(string(take().str))));

			if (peekEnumItem())
				return enums[string(take().str)];

			throw runtime_error(where(here) + ": expected static offset: " + tokenDump(peek()));
		};

		auto tryTakeComma = [&]() {
			if (isComma(peek())) take();
		};

		auto tryTakeSemiColon = [&]() {
			if (isSemiColon(peek())) take();
		};

		auto compileSpan = [&](auto& variable, bool global) {
			code.emplace_back(global ? Opcode::GSpan: Opcode::LSpan);
			code.emplace_back(variable.offset);
			code.emplace_back(variable.width);
		};

		struct FoundVariable {
			VariableDefinition* variable = nullptr;
			bool global = false;
		};

		auto findVariable = [&](const string& name) -> FoundVariable {
			assert(blocks.size());
			for (int i = int(blocks.size())-1; i >= 0; --i) {
				if (blocks[i]->variables.find(name) != blocks[i]->variables.end())
					return {&blocks[i]->variables[name], i == 0};
			}
			return {};
		};

		auto takeBlock = [&]() {
			if (isOpeningBrace(peek())) {
				takeOpeningBrace();
				while (!isClosingBrace(peek())) {
					takeStatement();
				}
				takeClosingBrace();
			}
			else {
				takeStatement();
			}
		};

		takeExpression = [&](Type rtype) {
			here = cursor.front();

			// takeExpression(Any) lazily converts to the first type that is found.
			// Eg, 1+1.0 will fail because the leading integer enforces the type for
			// the remainder of the expression. All casts must be explicit
			auto typeCheck = [&](Type type) {
				if (rtype == Any) rtype = type;

				if (rtype != type)
					throw runtime_error(where(here) + " found " + typeName(type) + "; expected " + typeName(rtype));
			};

			enum class Operator {
				Add,
				Sub,
				Mul,
				Div,
				Mod,
				Lt,
				Gt,
				Lte,
				Gte,
				Eq,
				NEq,
			};

			// shunting yard
			vector<Operator> operators;

			auto operatorSymbol = [](Operator op) {
				switch (op) {
					case Operator::Add: return "+";
					case Operator::Sub: return "-";
					case Operator::Mul: return "*";
					case Operator::Div: return "/";
					case Operator::Mod: return "%";
					case Operator::Lt: return "<";
					case Operator::Gt: return ">";
					case Operator::Lte: return "<=";
					case Operator::Gte: return ">=";
					case Operator::Eq: return "==";
					case Operator::NEq: return "!=";
				}
			};

			auto operatorPrecedence = [](Operator op) -> int {
				switch (op) {
					case Operator::Add: return 2;
					case Operator::Sub: return 2;
					case Operator::Mul: return 3;
					case Operator::Div: return 3;
					case Operator::Mod: return 3;
					case Operator::Lt: return 1;
					case Operator::Gt: return 1;
					case Operator::Lte: return 1;
					case Operator::Gte: return 1;
					case Operator::Eq: return 1;
					case Operator::NEq: return 1;
				}
				throw;
			};

			auto opcodeInteger = [&](Operator op, Opcode i) {
				if (rtype == Integer) return i;
				throw runtime_error(where(here) + ": no opcode for operator " + operatorSymbol(op) + " on type " + typeName(rtype));
			};

			auto opcodeIntegerFloat = [&](Operator op, Opcode i, Opcode f) {
				switch (rtype) {
					case Integer: return i;
					case Float: return f;
					default: break;
				}
				throw runtime_error(where(here) + ": no opcode for operator " + operatorSymbol(op) + " on type " + typeName(rtype));
			};

			auto opcodeIntegerFloatString = [&](Operator op, Opcode i, Opcode f, Opcode s) {
				switch (rtype) {
					case Integer: return i;
					case Float: return f;
					case String: return s;
					default: break;
				}
				throw runtime_error(where(here) + ": no opcode for operator " + operatorSymbol(op) + " on type " + typeName(rtype));
			};

			auto operatorToOpcode = [&](Operator op) -> Opcode {
				switch (op) {
					case Operator::Add: return opcodeIntegerFloatString(op, Opcode::AddI, Opcode::AddF, Opcode::AddS);
					case Operator::Sub: return opcodeIntegerFloat(op, Opcode::SubI, Opcode::SubF);
					case Operator::Mul: return opcodeIntegerFloat(op, Opcode::MulI, Opcode::MulF);
					case Operator::Div: return opcodeIntegerFloat(op, Opcode::DivI, Opcode::DivF);
					case Operator::Mod: return opcodeInteger(op, Opcode::ModI);
					case Operator::Lt: return Opcode::Lt;
					case Operator::Gt: return Opcode::Gt;
					case Operator::Lte: return Opcode::Lte;
					case Operator::Gte: return Opcode::Gte;
					case Operator::Eq: return Opcode::Eq;
					case Operator::NEq: return Opcode::NEq;
				}
				throw;
			};

			// shunting yard. Compile any previous operators per precedence, and cache the new one
			auto pushOperator = [&](Operator op) {
				while (operators.size() && operatorPrecedence(operators.back()) >= operatorPrecedence(op)) {
					code.emplace_back(operatorToOpcode(operators.back()));
					operators.pop_back();
				}
				operators.push_back(op);
			};

			auto peekOperator = [&]() -> string {
				// The tokeniser has already split two-char operators into separate tokens
				if (cursor.size() > 1 && cursor[0].type == Token::Punct && cursor[1].type == Token::Punct) {
					char a = cursor[0].str.front();
					char b = cursor[1].str.front();
					if (a == '=' && b == '=') { return "=="; }
					if (a == '<' && b == '=') { return "<="; }
					if (a == '>' && b == '=') { return ">="; }
					if (a == '!' && b == '=') { return "!="; }
				}
				if (cursor.size() > 0 && cursor[0].type == Token::Punct) {
					char a = cursor[0].str.front();
					if (a == '+') { return "+"; }
					if (a == '-') { return "-"; }
					if (a == '*') { return "*"; }
					if (a == '/') { return "/"; }
					if (a == '%') { return "%"; }
					if (a == '<') { return "<"; }
					if (a == '>') { return ">"; }
				}
				return "";
			};

			auto compileFunctionCall = [&](FunctionDefinition& func) {
				takeOpeningParen();

				for (auto& atype: func.args) {
					if (isClosingParen(peek()))
						throw runtime_error(where(here) + ": expected " + std::to_string(func.args.size()) + " arguments");
					takeExpression(atype);
				}

				takeClosingParen();
				typeCheck(func.type);

				code.emplace_back(Opcode::Call);
				// Run-time has no return/argument type info retained, just the mechanics
				code.emplace_back(Function{func.entry,func.frame,Size(func.args.size())});
			};

			auto compileVariableReference = [&](auto& variable, bool global) {
				if (variable.type > SpanTypes) {
					code.emplace_back(global ? Opcode::GSpan: Opcode::LSpan);
					code.emplace_back(variable.offset);
					code.emplace_back(variable.width);
				}
				else {
					code.emplace_back(global ? Opcode::GFetch: Opcode::LFetch);
					code.emplace_back(variable.offset);
				}

				if (isOpeningSquareBracket(peek())) {
					// It's a span cell access
					take();
					typeCheck(fromSpanType(variable.type));
					takeExpression(Integer);
					code.emplace_back(Opcode::Get);
					takeClosingSquareBracket();
				}
				else {
					// It's a regular scalar
					typeCheck(variable.type);
				}
			};

			// shunting yard loop
			do {
				here = cursor.front();
				string op = peekOperator();

				if (rtype != Any && op.size()) {
					for (size_t i = 0; i < op.size(); i++) take();

					if (op == "+") { pushOperator(Operator::Add); }
					else if (op == "-") { pushOperator(Operator::Sub); }
					else if (op == "*") { pushOperator(Operator::Mul); }
					else if (op == "/") { pushOperator(Operator::Div); }
					else if (op == "%") { pushOperator(Operator::Mod); }
					else if (op == "<") { pushOperator(Operator::Lt); }
					else if (op == ">") { pushOperator(Operator::Gt); }
					else if (op == "<=") { pushOperator(Operator::Lte); }
					else if (op == ">=") { pushOperator(Operator::Gte); }
					else if (op == "==") { pushOperator(Operator::Eq); }
					else if (op == "!=") { pushOperator(Operator::NEq); }
					else throw runtime_error(where(here) + ": unexpected operator " + op + " for type " + typeName(rtype));
				}

				// Nested parens. The inner expression must match our type
				if (isOpeningParen(peek())) {
					take();
					typeCheck(takeExpression(rtype));
					takeClosingParen();
					continue;
				}

				if (peekEnumItem()) {
					typeCheck(Integer);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(I(enums[string(take().str)]));
					continue;
				}

				if (peekBool()) {
					typeCheck(Bool);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(take().str == "true");
					continue;
				}

				if (peekIntegerLiteral()) {
					typeCheck(Integer);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(I(stoi(string(take().str))));
					continue;
				}

				if (peekFloatLiteral()) {
					typeCheck(Float);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(F(stof(string(take().str))));
					continue;
				}

				if (peekStringLiteral()) {
					typeCheck(String);
					string lit;
					string_view str = take().str;
					str.remove_prefix(1);
					str.remove_suffix(1);
					for (auto it = str.begin(); it != str.end(); ) {
						char c = *it++;
						if (c == '\\') {
							c = *it++;
							if (c == 'r') c = '\r';
							if (c == 'n') c = '\n';
							if (c == 't') c = '\t';
						}
						lit += c;
					}
					code.emplace_back(Opcode::Literal);
					code.emplace_back(S(lit));
					continue;
				}

				if (peekCast()) {
					Type type = takeType();
					typeCheck(type);

					takeOpeningParen();
					auto cast = takeExpression(Any);
					takeClosingParen();

					if (type != cast) {
						if (type == Integer && cast == Float) {
							code.emplace_back(Opcode::ICast);
							continue;
						}
						if (type == Float && cast == Integer) {
							code.emplace_back(Opcode::FCast);
							continue;
						}
						if (type == Bool) {
							code.emplace_back(Opcode::BCast);
							continue;
						}
						throw runtime_error(where(here) + ": cannot cast from " + typeName(cast) + " to " + typeName	(type));
					}
					continue;
				}

				if (peekName()) {
					string name(take().str);
					auto found = findVariable(name);

					if (found.variable) {
						compileVariableReference(*found.variable, found.global);
						continue;
					}

					if (functions.find(name) != functions.end()) {
						auto& func = functions[name];
						compileFunctionCall(func);
						continue;
					}

					if (callouts.find(name) != callouts.end()) {
						auto& func = callouts[name];

						takeOpeningParen();

						for (auto& atype: func.args) {
							if (isClosingParen(peek()))
								throw runtime_error(where(here) + ": expected " + std::to_string(func.args.size()) + " arguments");
							takeExpression(atype);
						}

						takeClosingParen();
						typeCheck(func.type);
						code.emplace_back(Opcode::CallOut);
						code.emplace_back(&func);
						continue;
					}

					throw runtime_error(where(here) + ": unknown symbol " + name);
					return Any;
				}

				throw runtime_error(where(here) + ": unknown token " + tokenDump(peek()));
				return Any;
			}
			while (peekOperator().size());

			// resolve remaining cached operators
			while (operators.size()) {
				code.emplace_back(operatorToOpcode(operators.back()));
				operators.pop_back();
			}

			tryTakeComma();
			return rtype;
		};

		auto takeVariableDefinition = [&]() {
			Type type = takeType();
			string name(take().str);

			if (currentBlock().variables.find(name) != currentBlock().variables.end())
				throw runtime_error(where(here) + ": duplicate name in scope: " + name);

			auto& variable = currentBlock().variables[name];
			variable.name = name;
			variable.type = type;
			variable.offset = currentFunc->frame++;

			if (isAssignment(peek())) {
				take();
				takeExpression(variable.type);
				code.emplace_back(Opcode::LStore);
				code.emplace_back(variable.offset);
			}

			takeSemiColon();
		};

		auto takeVariableMutation = [&]() {
			string name(take().str);
			takeAssignment();

			auto found = findVariable(name);

			if (found.variable) {
				auto& variable = *found.variable;
				takeExpression(variable.type);
				code.emplace_back(found.global ? Opcode::GStore: Opcode::LStore);
				code.emplace_back(variable.offset);
				tryTakeSemiColon();
				return;
			}

			throw runtime_error(where(here) + ": unknown variable: " + name);
		};

		// += -= *= /=
		auto takeVariableMutationAssign = [&]() {
			string name(take().str);
			string op(take().str); // +*-/
			takeAssignment();

			auto found = findVariable(name);

			if (found.variable) {
				auto& variable = *found.variable;
				code.emplace_back(found.global ? Opcode::GFetch: Opcode::LFetch);
				code.emplace_back(variable.offset);
				takeExpression(variable.type);

				auto error = [&]() {
					throw runtime_error(where(here) + ": unsupported " + op + "= type " + typeName(variable.type));
				};

				switch (variable.type) {
					case Integer: {
						switch (op.front()) {
							case '+': code.emplace_back(Opcode::AddI); break;
							case '-': code.emplace_back(Opcode::SubI); break;
							case '*': code.emplace_back(Opcode::MulI); break;
							case '/': code.emplace_back(Opcode::DivI); break;
						}
						break;
					}
					case Float: {
						switch (op.front()) {
							case '+': code.emplace_back(Opcode::AddF); break;
							case '-': code.emplace_back(Opcode::SubF); break;
							case '*': code.emplace_back(Opcode::MulF); break;
							case '/': code.emplace_back(Opcode::DivF); break;
						}
						break;
					}
					case String: {
						switch (op.front()) {
							case '+': code.emplace_back(Opcode::AddF); break;
							default: error();
						}
						break;
					}
					default: error();
				}
				code.emplace_back(found.global ? Opcode::GStore: Opcode::LStore);
				code.emplace_back(variable.offset);
				tryTakeSemiColon();
				return;
			}

			throw runtime_error(where(here) + ": unknown variable: " + name);
		};

		auto takeArrayDefinition = [&]() {
			Type type = takeType();
			string name(take().str);

			if (currentBlock().variables.find(name) != currentBlock().variables.end())
				throw runtime_error(where(here) + ": duplicate name in scope: " + name);

			auto& variable = currentBlock().variables[name];
			variable.name = name;
			variable.type = toSpanType(type);
			variable.offset = currentFunc->frame;

			takeOpeningSquareBracket();
			variable.width = takeStaticOffset();
			currentFunc->frame += variable.width;
			takeClosingSquareBracket();

			if (isAssignment(peek())) {
				take();
				takeOpeningSquareBracket();
				while (!isClosingSquareBracket(peek())) {
					here = cursor.front();

					compileSpan(variable, blocks.size() == 1);

					Size index = takeStaticOffset();

					if (index >= variable.width)
						throw runtime_error(where(here) + ": array index out of bounds: " + tokenDump(peek()));

					code.emplace_back(Opcode::Literal);
					code.emplace_back(I(index));

					takeAssignment();

					takeExpression(fromSpanType(variable.type));
					code.emplace_back(Opcode::Set);
					tryTakeComma();
				}
				takeClosingSquareBracket();
			}

			takeSemiColon();
		};

		auto takeSpanMutation = [&]() {
			string name(take().str);
			auto found = findVariable(name);

			if (!found.variable)
				throw runtime_error(where(here) + ": unknown variable: " + name);

			auto& variable = *found.variable;
			compileSpan(variable, found.global);

			takeOpeningSquareBracket();
			takeExpression(Integer); // index
			takeClosingSquareBracket();

			takeAssignment();

			takeExpression(fromSpanType(variable.type));
			code.emplace_back(Opcode::Set);
			tryTakeSemiColon();
		};

		auto takePrint = [&]() {
			take();
			takeOpeningParen();
			while (peek() && !isClosingParen(peek()) && !isSemiColon(peek())) {
				takeExpression(Any);
				code.emplace_back(Opcode::Print);
			}
			takeClosingParen();
			takeSemiColon();
		};

		auto takeReturn = [&]() {
			take();

			if (currentFunc->type == Any && !isSemiColon(peek()))
				throw runtime_error(where(here) + ": returning value in void function");

			// everything returns something, even void functions, so insert an int(0)
			if (isSemiColon(peek())) {
				code.emplace_back(Opcode::Literal);
				code.emplace_back(I(0));
			}
			else {
				takeExpression(currentFunc->type);
			}

			code.emplace_back(Opcode::Return);
			takeSemiColon();
		};

		auto takeEnumDefinition = [&]() {
			take();
			takeOpeningBrace();
			while (!isClosingBrace(peek())) {
				if (!peekName())
					throw runtime_error(where(here) + ": expected name: " + tokenDump(peek()));
				string name(take().str);
				if (enums.find(name) != enums.end())
					throw runtime_error(where(here) + ": duplicate enum item: " + name);
				enums[name] = Size(enums.size());
				tryTakeComma();
			}
			takeClosingBrace();
			takeSemiColon();
		};

		auto takeIf = [&]() {
			take();
			takeOpeningParen();
			takeExpression(Any);
			code.emplace_back(Opcode::JZ);
			Size ifJump = code.size();
			code.emplace_back(Size(0));
			takeClosingParen();
			takeBlock();

			if (peekElse()) {
				take();
				code.emplace_back(Opcode::Jump);
				Size elseJump = code.size();
				code.emplace_back(Size(0));
				code[ifJump] = Size(code.size());
				if (peekIf()) takeStatement(); else takeBlock();
				code[elseJump] = Size(code.size());
			}
			else {
				code[ifJump] = Size(code.size());
			}
		};

		auto takeFor = [&]() {
			take();
			beginBlock();

			takeOpeningParen();
			takeStatement();

			code.emplace_back(Opcode::Jump);
			Size start = code.size();
			code.emplace_back();

			Size begin = code.size();
				auto condition = cursor;
				takeExpression(Any);
				takeSemiColon();
				auto advance = cursor;
				takeStatement();
				takeClosingParen();
				auto body = cursor;
			code.resize(begin);

			cursor = advance;
			takeStatement();

			code[start] = Size(code.size());

			cursor = condition;
			takeExpression(Any);
			code.emplace_back(Opcode::JZ);
			Size done = code.size();
			code.emplace_back();

			cursor = body;
			takeBlock();
			code.emplace_back(Opcode::Jump);
			code.emplace_back(Size(begin));
			code[done] = Size(code.size());

			endBlock(begin, code.size());
		};

		auto takeWhile = [&]() {
			take();
			beginBlock();

			Size begin = code.size();
			takeOpeningParen();
			takeExpression(Any);
			takeClosingParen();
			code.emplace_back(Opcode::JZ);
			Size done = code.size();
			code.emplace_back();
			takeBlock();
			code.emplace_back(Opcode::Jump);
			code.emplace_back(begin);
			code[done] = Size(code.size());

			endBlock(begin, code.size());
		};

		auto takeContinue = [&]() {
			take();
			code.emplace_back(Opcode::Jump);
			currentBlock().cjumps.push_back(code.size());
			code.emplace_back();
			takeSemiColon();
		};

		auto takeBreak = [&]() {
			take();
			code.emplace_back(Opcode::Jump);
			currentBlock().bjumps.push_back(code.size());
			code.emplace_back();
			takeSemiColon();
		};

		auto takeFunctionCall = [&]() {
			takeExpression(Any);
			code.emplace_back(Opcode::Drop);
			takeSemiColon();
		};

		takeStatement = [&]() {
			auto here = cursor.front();

			if (peekVariableDefinition()) {
				takeVariableDefinition();
				return;
			}

			if (peekVariableMutation()) {
				takeVariableMutation();
				return;
			}

			if (peekVariableMutationAssign()) {
				takeVariableMutationAssign();
				return;
			}

			if (peekArrayDefinition()) {
				takeArrayDefinition();
				return;
			}

			if (peekSpanMutation()) {
				takeSpanMutation();
				return;
			}

			if (peekEnumDefinition()) {
				takeEnumDefinition();
				return;
			}

			if (peekPrint()) {
				takePrint();
				return;
			}

			if (peekReturn()) {
				takeReturn();
				return;
			}

			if (peekIf()) {
				takeIf();
				return;
			}

			if (peekFor()) {
				takeFor();
				return;
			}

			if (peekWhile()) {
				takeWhile();
				return;
			}

			if (peekContinue()) {
				takeContinue();
				return;
			}

			if (peekBreak()) {
				takeBreak();
				return;
			}

			if (peekFunctionCall()) {
				takeFunctionCall();
				return;
			}

			throw runtime_error(where(here) + ": unexpected takeStatement: " + tokenDump(peek()));
		};

		auto takeFunctionDefinition = [&]() {
			// The global function is executed once to initialize global state.
			// Include a jump around each normal function's code segment
			code.emplace_back(Opcode::Jump);
			Size skip = Size(code.size());
			code.emplace_back();

			Type rtype = takeType();
			string name(take().str);

			if (functions.find(name) != functions.end())
				throw runtime_error(where(here) + ": duplicate function name: " + name);

			currentFunc = &functions[name];
			currentFunc->name = name;
			currentFunc->type = rtype;
			currentFunc->entry = code.size();
			currentFunc->args.clear();
			currentFunc->frame = 0;

			beginBlock();

			// Argument list
			takeOpeningParen();
			while (peek() && isType(peek())) {
				currentFunc->args.push_back(takeType());
				currentFunc->frame++;
				if (peekName()) {
					string name(take().str);
					auto& variable = currentBlock().variables[name];
					variable.name = name;
					variable.type = currentFunc->args.back();
					variable.offset = currentFunc->frame++;
					tryTakeComma();
					continue;
				}
				throw runtime_error(where(here) + ": expected argument name: " + tokenDump(peek()));
			}
			takeClosingParen();

			takeBlock();

			bool haveReturn = holds_alternative<Opcode>(code.back()) && get<Opcode>(code.back()) == Opcode::Return;

			// void functions that have no explicit return statement still return something
			// so the call/return stack handling balances out
			if (!haveReturn) {
				code.emplace_back(Opcode::Literal);
				code.emplace_back(I(0));
				code.emplace_back(Opcode::Return);
			}

			endBlock(currentFunc->entry, code.size());

			code[skip] = Size(code.size());
			currentFunc = globalFunction;
		};

		beginBlock();
		// Parse the top-level global function. Everything else is recursive descent
		while (peek()) {
			here = cursor.front();

			if (peekFunctionDefinition()) {
				takeFunctionDefinition();
				continue;
			}

			takeStatement();
		}
		// no endBlock() for global

		// Size of the global stack frame that will persist across ::execute() invocations
		globalFrame = currentFunc->frame;

		callins.clear();
		for (auto& [name,def]: functions) {
			callins[name] = CallIn{def.entry,def.args,def.frame};
		}
	}

	// Runtime data stack; frames are contiguous
	std::vector<Value> stack;

	// Runtime frame "pointers" (offsets into stack vector)
	std::vector<Size> frames;

	// Return "addresses" (offsets into code vector)
	std::vector<Size> rstack;

	// A view of the current frame within the data stack
	std::span<Value> frame() {
		return {&stack[frames.back()], stack.size()-frames.back()};
	}

	void push(Value v) {
		stack.push_back(v);
	}

	Value pop() {
		Value v = stack.back();
		stack.pop_back();
		return v;
	}

	Value top() {
		return stack.back();
	}

	// For std::visit
	struct {
		// Essentially a flag register
		bool b = false;
		void operator()(const bool& bb) { b = bb; }
		void operator()(const I& i) { b = i != 0; }
		void operator()(const F& f) { b = f < 0 || f > 0; }
		void operator()(const S& s) { b = s.size() > 0; }
		void operator()(const U& s) { b = false; }
		void operator()(const Span& s) { b = s.width > 0; }
	} boolCaster;

	// Instruction "pointer" (offset into code vector)
	Size instruction = 0;

	// Execute the next instruction
	void next() {
		using namespace std;
		auto op = get<Opcode>(code[instruction++]);

		switch (op) {
			case Opcode::Call: {
				Function fn = get<Function>(code[instruction++]);
				rstack.push_back(instruction);
				instruction = fn.entry;
				frames.push_back(stack.size() - fn.args);
				stack.resize(frames.back() + fn.frame, Value(I(0)));
				break;
			}
			case Opcode::Return: {
				Value v = stack.back();
				stack.resize(frames.back());
				frames.pop_back();
				stack.push_back(v);
				instruction = rstack.back();
				rstack.pop_back();
				break;
			}
			case Opcode::Jump: {
				instruction = get<Size>(code[instruction++]);
				break;
			}
			case Opcode::JZ: {
				Size to = get<Size>(code[instruction++]);
				visit(boolCaster,pop());
				if (!boolCaster.b) instruction = to;
				break;
			}
			case Opcode::CallOut: {
				CallOut* f = get<CallOut*>(code[instruction++]);
				Value r = f->fn(*this, span<Value>(stack.begin() + stack.size() - f->args.size(), f->args.size()));
				stack.resize(stack.size()-f->args.size());
				push(r);
				break;
			}
			case Opcode::LStore: {
				Size o = get<Size>(code[instruction++]);
				frame()[o] = pop();
				break;
			}
			case Opcode::LFetch: {
				Size o = get<Size>(code[instruction++]);
				push(frame()[o]);
				break;
			}
			case Opcode::GStore: {
				Size o = get<Size>(code[instruction++]);
				stack[o] = pop();
				break;
			}
			case Opcode::GFetch: {
				Size o = get<Size>(code[instruction++]);
				push(stack[o]);
				break;
			}
			case Opcode::Print: {
				Value v = pop();
				cout << v;
				break;
			}
			case Opcode::ICast: {
				push(I(get<F>(pop())));
				break;
			}
			case Opcode::FCast: {
				push(F(get<I>(pop())));
				break;
			}
			case Opcode::BCast: {
				visit(boolCaster,pop());
				push(boolCaster.b);
				break;
			}
			case Opcode::AddI: {
				I b = get<I>(pop());
				I a = get<I>(pop());
				push(a+b);
				break;
			}
			case Opcode::AddF: {
				F b = get<F>(pop());
				F a = get<F>(pop());
				push(a+b);
				break;
			}
			case Opcode::AddS: {
				S b = get<S>(pop());
				S a = get<S>(pop());
				push(a+b);
				break;
			}
			case Opcode::SubI: {
				I b = get<I>(pop());
				I a = get<I>(pop());
				push(a-b);
				break;
			}
			case Opcode::SubF: {
				F b = get<F>(pop());
				F a = get<F>(pop());
				push(a-b);
				break;
			}
			case Opcode::MulI: {
				I b = get<I>(pop());
				I a = get<I>(pop());
				push(a*b);
				break;
			}
			case Opcode::MulF: {
				F b = get<F>(pop());
				F a = get<F>(pop());
				push(a*b);
				break;
			}
			case Opcode::DivI: {
				I b = get<I>(pop());
				I a = get<I>(pop());
				push(a/b);
				break;
			}
			case Opcode::DivF: {
				F b = get<F>(pop());
				F a = get<F>(pop());
				push(a/b);
				break;
			}
			case Opcode::ModI: {
				I b = get<I>(pop());
				I a = get<I>(pop());
				push(a%b);
				break;
			}
			case Opcode::Lt: {
				Value b = pop();
				Value a = pop();
				push(a<b);
				break;
			}
			case Opcode::Gt: {
				Value b = pop();
				Value a = pop();
				push(a>b);
				break;
			}
			case Opcode::Lte: {
				Value b = pop();
				Value a = pop();
				push(a<=b);
				break;
			}
			case Opcode::Gte: {
				Value b = pop();
				Value a = pop();
				push(a>=b);
				break;
			}
			case Opcode::Eq: {
				Value b = pop();
				Value a = pop();
				push(a==b);
				break;
			}
			case Opcode::NEq: {
				Value b = pop();
				Value a = pop();
				push(!(a==b));
				break;
			}
			case Opcode::Literal: {
				auto v = code[instruction++];
				struct Pusher {
					WESL& vm;
					Pusher(WESL& v) : vm{v} {}
					void operator()(const Opcode& v) { throw; }
					void operator()(const Size& v) { throw; }
					void operator()(const bool& v) { vm.push(v); }
					void operator()(const I& v) { vm.push(v); }
					void operator()(const S& v) { vm.push(v); }
					void operator()(const F& v) { vm.push(v); }
					void operator()(const Function& v) { throw; }
					void operator()(const CallOut* v) { throw; }
				} pusher(*this);
				visit(pusher, v);
				break;
			}
			case Opcode::Drop: {
				pop();
				break;
			}
			case Opcode::LSpan: {
				Size off = get<Size>(code[instruction++]);
				Size len = get<Size>(code[instruction++]);
				push(Span{frames.back()+off,len});
				break;
			}
			case Opcode::GSpan: {
				Size off = get<Size>(code[instruction++]);
				Size len = get<Size>(code[instruction++]);
				push(Span{off,len});
				break;
			}
			case Opcode::Get: {
				Size off = max(I(0), get<I>(pop()));
				Span spn = get<Span>(pop());
				Value v(I(0));
				if (spn.width > 0) {
					off = min(spn.width-1, off);
					if (spn.start + off < stack.size()) {
						v = stack[spn.start + off];
					}
				}
				push(v);
				break;
			}
			case Opcode::Set: {
				Value v = pop();
				Size off = max(I(0), get<I>(pop()));
				Span spn = get<Span>(pop());
				if (spn.width > 0) {
					off = min(spn.width-1, off);
					if (spn.start + off < stack.size()) {
						stack[spn.start + off] = v;
					}
				}
				break;
			}
		}
	}

public:
	void reset() {
		stack.clear();
		frames.clear();
		instruction = 0;
	}

	void clear() {
		reset();
		code.clear();
	}

	struct Result {
		bool ok = false;
		std::string error;
		Value value;
	};

	// Register an external callback
	void callback(std::string_view name, Type type, std::vector<Type> args, std::function<Value(WESL&,std::span<Value>)> fn) {
		callouts[std::string(name)] = {type,args,fn};
	}

	// Convert a stack-relative Span{} to an absolute span<Value>
	std::span<Value> absolute(Span s) {
		return std::span<Value>(stack.data() + s.start, s.width);
	}

	Result compile(std::string_view src) {
		clear();
		try {
			processSourceCode(src);
		}
		catch(std::exception& e) {
			return {false,e.what(),Value()};
		}
		return {true,"",Value()};
	}

	Result execute(std::string_view func, std::span<std::variant<I,F,S,U>> args) {
		using namespace std;

		try
		{
			if (!frames.size()) {
				frames.push_back(0);
				stack.resize(globalFrame, Value(I(0)));
				instruction = 0;
				while (instruction < Size(code.size())) next();
				assert(stack.size() == globalFrame);
			}

			auto it = callins.find(string(func));
			if (it != callins.end()) {
				auto& func = it->second;
				assert(stack.size() == globalFrame);
				auto depth = rstack.size();
				rstack.push_back(instruction);
				instruction = func.entry;
				frames.push_back(stack.size());
				stack.resize(frames.back() + func.frame, Value(I(0)));
				if (args.size() != func.args.size())
					throw runtime_error("found " + std::to_string(int(args.size())) + " arguments; expected " + std::to_string(int(func.args.size())));
				for (size_t i = 0; i < func.args.size(); i++) {
					switch (func.args[i]) {
						case Integer: {
							stack[i] = get<I>(args[i]);
							break;
						}
						case Float: {
							stack[i] = get<F>(args[i]);
							break;
						}
						case String: {
							stack[i] = get<S>(args[i]);
							break;
						}
						case UserData: {
							stack[i] = get<U>(args[i]);
							break;
						}
						default: {
							throw runtime_error("unsupported call arg[" + typeName(func.args[i]) + "]");
						}
					}
				}
				while (rstack.size() > depth) next();
				Value ret = stack.back();
				stack.pop_back();
				assert(stack.size() == globalFrame);
				return {true,"",ret};
			}
		}
		catch (exception& e)
		{
			return {false,e.what(),Value()};
		}

		return {false,"unknown function: " + string(func),Value()};
	}

	typedef std::function<std::string(const I&)> ISerialize;
	typedef std::function<void(I&, const std::string&)> IUnserialize;

	typedef std::function<std::string(const F&)> FSerialize;
	typedef std::function<void(F&, const std::string&)> FUnserialize;

	typedef std::function<std::string(const S&)> SSerialize;
	typedef std::function<void(S&, const std::string&)> SUnserialize;

	typedef std::function<std::string(const U&)> UserDataSerialize;
	typedef std::function<void(U&, const std::string&)> UserDataUnserialize;

	std::string serialize(
		ISerialize iSerialize,
		FSerialize fSerialize,
		SSerialize sSerialize,
		USerialize uSerialize
	) {
		using namespace std;
		ostringstream state;

		auto encode = [&](const string& s) {
			return s;
		};

		state << "stack " << stack.size() << endl;
		for (auto& value: stack) {
			if (holds_alternative<bool>(value)) ss << "bool " << (get<bool>(value) ? "true": "false") << endl;
			if (holds_alternative<I>(value)) string s = iSerialize(get<I>(value)); ss << "I " << s.size() << ' ' << s << endl;
			if (holds_alternative<F>(value)) string s = fSerialize(get<F>(value)); ss << "F " << s.size() << ' ' << s << endl;
			if (holds_alternative<S>(value)) string s = sSerialize(get<S>(value)); ss << "S " << s.size() << ' ' << s << endl;
			if (holds_alternative<U>(value)) string s = uSerialize(get<U>(value)); ss << "U " << s.size() << ' ' << s << endl;
			if (holds_alternative<Span>(value)) ss << "Span " << get<Span>(value).start << ' ' get<Span>(value).width << endl;
		}

		state << "frames " << frames.size() << endl;
		for (auto offset: frames) {
			state << offset << endl;
		}

		state << "rstack " << rstack.size() << endl;
		for (auto offset: rstack) {
			state << offset << endl;
		}
	}

	void unserialize(const std::string& s,
		IUnserialize iUnserialize,
		FUnserialize fUnserialize,
		SUnserialize sUnserialize,
		UUnserialize uUnserialize
	) {
		using namespace std;
		istringstream state(s);

		auto decode = [&](const string& s) {
			return s;
		};

		string word;

		state >> word;
		if (word != "stack")
			throw runtime_error("expected stack");

		Size depth, size;
		string type;
		string value;

		state >> depth;
		for (Size i = 0; i < depth; i++) {
			state >> type;
			state >> size;

			if (type == "I") {
		}

	}
};
