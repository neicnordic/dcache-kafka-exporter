use std::borrow::Cow;
use regex::{Captures, Regex, Replacer};

type ReplacerFn = fn (caps: &Captures<'_>, dst: &mut String);

enum Replacement {
    Const(String),
    Dependent(ReplacerFn),
}

struct MessageRewriteRule {
    re: Regex,
    replacer: Replacement,
}

pub struct MessageRewriteRules {
    rules: Vec<MessageRewriteRule>,
}

impl Replacer for &Replacement {
    fn replace_append(&mut self, caps: &Captures<'_>, dst: &mut String) {
        match self {
            Replacement::Const(s) => { dst.push_str(s); }
            Replacement::Dependent(f) => { f(caps, dst); }
        }
    }
}

impl MessageRewriteRule {
    fn new(name: &str, re_str: &str, replacer: Option<ReplacerFn>) -> Self {
        let replacer =
            match replacer {
                None => { Replacement::Const(format!("<{}>", name)) }
                Some(replacer) => { Replacement::Dependent(replacer) }
            };
        Self {replacer: replacer, re: Regex::new(re_str).unwrap()}
    }
    fn rewrite<'h>(&self, msg: &'h str) -> Cow<'h, str> {
        return self.re.replace_all(msg, &self.replacer);
    }
}

fn domain_name_replacer(caps: &Captures<'_>, dst: &mut String) {
    if caps.name("last").unwrap().as_str().find(char::is_uppercase) != None {
        // Likely Java exception.
        dst.push_str(caps.get(0).unwrap().as_str());
    } else {
        dst.push_str("<domain-name>");
    }
}

// The order matters, e.g. integer must come after IP addresses.
const RULES : [(&str, &str, Option<ReplacerFn>); 18] = [
    ("url", r"\w+://[^[:space:]]+[^[:space:],.;:?()\[\]]", None),
    ("pool-name", r"PoolName=[[:alnum:]_-]+", None),
    ("pool-address", r"PoolAddress=[[:alnum]_@/-]+", None),
    ("quoted-ref", r">[[:alnum:]_@-]+<", None),
    ("date-and-time",
     r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun) \w{3} \d+ \d{2}:\d{2}:\d{2} \w+ \d{4}",
     None),
    ("checksum", r"\[\d+:\x+\]", None),
    ("ipv4-address-and-port",   r"\b\d+(\.\d+){3}:\d+",                 None),
    ("ipv4-address",            r"\b\d+(\.\d+){3}",                     None),
    ("ipv6-address-and-port",   r"\[[0-9a-f]+(:[0-9a-f]+)+\]:\d+\b",    None),
    ("ipv6-address",            r"\[[0-9a-f]+(:[0-9a-f]+)+\]",          None),
    ("ipv6-address",            r"\b[0-9a-f]+(:[0-9a-f]+)+",            None),
    ("dcache-cell", r"\[>[[:alnum:]_:.@-]*\]", None),
    ("size", r"\b\d+(\.\d+)? ([kMGTE]i?)?B\b", None),
    ("distinguished-name", r"\<(\w+=([^,]|\\,)+,)+(?i:CN|DC|C)=\w+\>", None),
    ("pnfsid", r"\b[0-9A-F]{36}\b", None),
    ("path", r"\B/[^ <>]+\b", None),
    ("dns-domain",
     r"(?x) \< ([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+
        (?<last>[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?) \>",
     Some(domain_name_replacer)),
    ("int", r"\b\d+\b", None),
];

impl MessageRewriteRules {
    pub fn new() -> Self {
        let rules = Vec::from(RULES.map(
            |(name, re, rep)| MessageRewriteRule::new(name, re, rep)));
        return MessageRewriteRules { rules: rules };
    }
    pub fn rewrite<'h>(&self, msg: &str) -> String {
        let acc = String::from(msg);
        self.rules.iter().fold(
            acc, |acc, rule| rule.rewrite(&acc).into_owned())
    }
}
