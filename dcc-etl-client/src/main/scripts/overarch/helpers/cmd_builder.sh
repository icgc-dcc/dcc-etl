#!/bin/bash
# Helps build a command that can be displayed nicely before being executed; see test at the bottom for usage
# ===========================================================================

function new_cmd_builder() {
	unset cmd
	unset cmd_builder
	cmd_builder="# cmd:"
}

function add_to_cmd() {
	value=${1?}
	if [ -z "$cmd_builder" ]; then
		echo "ERROR: unitialized instance, use \"new_cmd_builder\" first"
		exit 1
	fi
	if [[ "${cmd_builder?}" =~ \n ]]; then
		cmd_builder="${cmd_builder?} \\\\\n"
	else
		cmd_builder="${cmd_builder?} \n"
	fi
	cmd_builder="${cmd_builder?}${value?}"
}

function build_cmd() {
	echo "${cmd_builder?}"
	unset cmd_builder
}

function pretty_print_cmd() { # TODO: sanity checks
	cmd=${1?}
	echo -e "\n${cmd?}\n"
}

function eval_cmd() { # TODO: sanity checks
	cmd=${1?}
	cmd=$(echo -e "${cmd?}" | awk '!/^#/')
	cmd=${cmd//\\t/}
	cmd=${cmd//\\/}
	cmd=$(echo -e "${cmd?}" | awk '!/^[ ]*$/')
	cmd=${cmd//\\n/}
	eval ${cmd?}
}

# ---------------------------------------------------------------------------

if [ "$1" == "test" ]; then
	new_cmd_builder
	add_to_cmd "echo"
	add_to_cmd "  hello"
	add_to_cmd "  world1"
	cmd=$(build_cmd)
	pretty_print_cmd "${cmd?}"
	eval_cmd "${cmd?}"

	new_cmd_builder
	add_to_cmd "echo"
	add_to_cmd "  hello"
	add_to_cmd "  world2"
	cmd=$(build_cmd)
	pretty_print_cmd "${cmd?}"
	eval_cmd "${cmd?}"
fi

# ===========================================================================

