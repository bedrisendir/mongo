/**
 * Copyright 2016 (c) 10gen Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/db/pipeline/value.h"

namespace mongo {

using boost::intrusive_ptr;

/**
 * This class implements the transformation logic for the $replaceRoot stage.
 */
class ReplaceRootTransformation final
    : public DocumentSourceSingleDocumentTransformation::TransformerInterface {

public:
    ReplaceRootTransformation() {}

    Document applyTransformation(Document input) {
        // Extract subdocument in the form of a Value.
        _variables->setRoot(input);
        Value newRoot = _newRoot->evaluate(_variables.get());

        // The newRoot expression must evaluate to a valid Value.
        uassert(
            40232,
            str::stream() << " 'newRoot' argument "
                          << " to $replaceRoot stage must be able to be evaluated by the document "
                          << input.toString()
                          << ", try ensuring that your field path(s) exist by prepending a "
                          << "$match: {<path>: $exists} aggregation stage.",
            !newRoot.missing());

        // The newRoot expression, if it exists, must evaluate to an object.
        uassert(
            40228,
            str::stream()
                << " 'newRoot' argument to $replaceRoot stage must evaluate to an object, but got "
                << typeName(newRoot.getType())
                << " try ensuring that it evaluates to an object by prepending a "
                << "$match: {<path>: {$type: 'object'}} aggregation stage.",
            newRoot.getType() == Object);

        // Turn the value into a document.
        return newRoot.getDocument();
    }

    // Optimize the newRoot expression.
    void optimize() {
        _newRoot->optimize();
    }

    Document serialize(bool explain) const {
        return Document{{"newRoot", _newRoot->serialize(explain)}};
    }

    DocumentSource::GetDepsReturn addDependencies(DepsTracker* deps) const {
        _newRoot->addDependencies(deps);
        // This stage will replace the entire document with a new document, so any existing fields
        // will be replaced and cannot be required as dependencies.
        return DocumentSource::EXHAUSTIVE_FIELDS;
    }

    void injectExpressionContext(const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
        _newRoot->injectExpressionContext(pExpCtx);
    }

    // Create the replaceRoot transformer. Uasserts on invalid input.
    static std::unique_ptr<ReplaceRootTransformation> create(const BSONElement& spec) {

        // Confirm that the stage was called with an object.
        uassert(40229,
                str::stream() << "expected an object as specification for $replaceRoot stage, got "
                              << typeName(spec.type()),
                spec.type() == Object);

        // Create the pointer, parse the stage, and return.
        std::unique_ptr<ReplaceRootTransformation> parsedReplaceRoot =
            stdx::make_unique<ReplaceRootTransformation>();
        parsedReplaceRoot->parse(spec);
        return parsedReplaceRoot;
    }

    // Check for valid replaceRoot options, and populate internal state variables.
    void parse(const BSONElement& spec) {
        // We need a VariablesParseState in order to parse the 'newRoot' expression.
        VariablesIdGenerator idGenerator;
        VariablesParseState vps(&idGenerator);

        // Get the options from this stage. Currently the only option is newRoot.
        for (auto&& argument : spec.Obj()) {
            const StringData argName = argument.fieldNameStringData();

            if (argName == "newRoot") {
                // Allows for field path, object, and other expressions.
                _newRoot = Expression::parseOperand(argument, vps);
            } else {
                uasserted(40230,
                          str::stream() << "unrecognized option to $replaceRoot stage: " << argName
                                        << ", only valid option is 'newRoot'.");
            }
        }

        // Check that there was a new root specified.
        uassert(40231, "no newRoot specified for the $replaceRoot stage", _newRoot);
        _variables = stdx::make_unique<Variables>(idGenerator.getIdCount());
    }

private:
    std::unique_ptr<Variables> _variables;
    boost::intrusive_ptr<Expression> _newRoot;
};

REGISTER_DOCUMENT_SOURCE(replaceRoot,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceReplaceRoot::createFromBson);

intrusive_ptr<DocumentSource> DocumentSourceReplaceRoot::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {

    return new DocumentSourceSingleDocumentTransformation(
        pExpCtx, ReplaceRootTransformation::create(elem), "$replaceRoot");
}

}  // namespace mongo
