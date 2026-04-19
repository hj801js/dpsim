/* Author: Christoph Wirtz <christoph.wirtz@fgh-ma.de>
 * SPDX-FileCopyrightText: 2025 FGH e.V.
 * SPDX-License-Identifier: MPL-2.0
 */

#include <dpsim-models/DP/DP_Ph1_PiLine.h>

using namespace CPS;

DP::Ph1::PiLine::PiLine(String uid, String name, Logger::Level logLevel)
    : Base::Ph1::PiLine(mAttributes),
      CompositePowerComp<Complex>(uid, name, true, true, logLevel) {
  setVirtualNodeNumber(1);
  setTerminalNumber(2);

  SPDLOG_LOGGER_INFO(mSLog, "Create {} {}", this->type(), name);
  **mIntfVoltage = MatrixComp::Zero(1, 1);
  **mIntfCurrent = MatrixComp::Zero(1, 1);
}

///DEPRECATED: Remove method
SimPowerComp<Complex>::Ptr DP::Ph1::PiLine::clone(String name) {
  auto copy = PiLine::make(name, mLogLevel);
  copy->setParameters(**mSeriesRes, **mSeriesInd, **mParallelCap,
                      **mParallelCond);
  return copy;
}

void DP::Ph1::PiLine::initializeFromNodesAndTerminals(Real frequency) {

  // Static calculation
  Real omega = 2. * PI * frequency;
  Complex impedance = {**mSeriesRes, omega * **mSeriesInd};
  (**mIntfVoltage)(0, 0) = initialSingleVoltage(1) - initialSingleVoltage(0);
  (**mIntfCurrent)(0, 0) = (**mIntfVoltage)(0, 0) / impedance;

  // Initialization of virtual node
  mVirtualNodes[0]->setInitialVoltage(initialSingleVoltage(0) +
                                      (**mIntfCurrent)(0, 0) * **mSeriesRes);

  // Negative R or admittance is a legitimate outcome of Kron reduction —
  // when internal nodes are eliminated, the reduced Y-matrix can have
  // negative real parts. We stamp them directly as normal Resistor /
  // Inductor; MNA handles negative conductance mathematically. Only the
  // exact R == 0 case needs the Transformer remap (converter-side, see
  // docs/08 §3) to avoid a singular MNA matrix.

  // Create series sub components (standard path — accepts R<0 and L<0)
  mSubSeriesResistor =
      std::make_shared<DP::Ph1::Resistor>(**mName + "_res", mLogLevel);
  mSubSeriesResistor->setParameters(**mSeriesRes);
  mSubSeriesResistor->connect({mTerminals[0]->node(), mVirtualNodes[0]});
  mSubSeriesResistor->initialize(mFrequencies);
  mSubSeriesResistor->initializeFromNodesAndTerminals(frequency);
  addMNASubComponent(mSubSeriesResistor,
                     MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT,
                     MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT, false);

  if (**mSeriesInd >= 0) {
    // Series inductor (standard case: X > 0 transmission line).
    mSubSeriesInductor =
        std::make_shared<DP::Ph1::Inductor>(**mName + "_ind", mLogLevel);
    mSubSeriesInductor->setParameters(**mSeriesInd);
    mSubSeriesInductor->connect({mVirtualNodes[0], mTerminals[1]->node()});
    mSubSeriesInductor->initialize(mFrequencies);
    mSubSeriesInductor->initializeFromNodesAndTerminals(frequency);
    addMNASubComponent(mSubSeriesInductor,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT, true);
  } else {
    // Series compensation (X < 0): stamp as a capacitor.
    // X = omega * L (negative L from Reader); equivalent series capacitor
    // satisfies X = -1/(omega * C), so C = -1 / (omega^2 * L).
    Real seriesCap = -1.0 / (omega * omega * **mSeriesInd);
    mSubSeriesCapacitor =
        std::make_shared<DP::Ph1::Capacitor>(**mName + "_cap_series", mLogLevel);
    mSubSeriesCapacitor->setParameters(seriesCap);
    mSubSeriesCapacitor->connect({mVirtualNodes[0], mTerminals[1]->node()});
    mSubSeriesCapacitor->initialize(mFrequencies);
    mSubSeriesCapacitor->initializeFromNodesAndTerminals(frequency);
    addMNASubComponent(mSubSeriesCapacitor,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT, true);
    SPDLOG_LOGGER_INFO(mSLog,
                       "Series compensation: L={} H < 0, using capacitor C={} F",
                       (float)**mSeriesInd, (float)seriesCap);
  }

  // By default there is always a small conductance to ground to
  // avoid problems with floating nodes.
  Real defaultParallelCond = 1e-6;
  **mParallelCond =
      (**mParallelCond > 0) ? **mParallelCond : defaultParallelCond;

  // Create parallel sub components
  mSubParallelResistor0 =
      std::make_shared<DP::Ph1::Resistor>(**mName + "_con0", mLogLevel);
  mSubParallelResistor0->setParameters(2. / **mParallelCond);
  mSubParallelResistor0->connect(
      SimNode::List{SimNode::GND, mTerminals[0]->node()});
  mSubParallelResistor0->initialize(mFrequencies);
  mSubParallelResistor0->initializeFromNodesAndTerminals(frequency);
  addMNASubComponent(mSubParallelResistor0,
                     MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT,
                     MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT, false);

  mSubParallelResistor1 =
      std::make_shared<DP::Ph1::Resistor>(**mName + "_con1", mLogLevel);
  mSubParallelResistor1->setParameters(2. / **mParallelCond);
  mSubParallelResistor1->connect(
      SimNode::List{SimNode::GND, mTerminals[1]->node()});
  mSubParallelResistor1->initialize(mFrequencies);
  mSubParallelResistor1->initializeFromNodesAndTerminals(frequency);
  addMNASubComponent(mSubParallelResistor1,
                     MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT,
                     MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT, false);

  if (**mParallelCap >= 0) {
    mSubParallelCapacitor0 =
        std::make_shared<DP::Ph1::Capacitor>(**mName + "_cap0", mLogLevel);
    mSubParallelCapacitor0->setParameters(**mParallelCap / 2.);
    mSubParallelCapacitor0->connect(
        SimNode::List{SimNode::GND, mTerminals[0]->node()});
    mSubParallelCapacitor0->initialize(mFrequencies);
    mSubParallelCapacitor0->initializeFromNodesAndTerminals(frequency);
    addMNASubComponent(mSubParallelCapacitor0,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT, true);

    mSubParallelCapacitor1 =
        std::make_shared<DP::Ph1::Capacitor>(**mName + "_cap1", mLogLevel);
    mSubParallelCapacitor1->setParameters(**mParallelCap / 2.);
    mSubParallelCapacitor1->connect(
        SimNode::List{SimNode::GND, mTerminals[1]->node()});
    mSubParallelCapacitor1->initialize(mFrequencies);
    mSubParallelCapacitor1->initializeFromNodesAndTerminals(frequency);
    addMNASubComponent(mSubParallelCapacitor1,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT,
                       MNA_SUBCOMP_TASK_ORDER::TASK_BEFORE_PARENT, true);
  }

  SPDLOG_LOGGER_INFO(
      mSLog,
      "\n--- Initialization from powerflow ---"
      "\nVoltage across: {:s}"
      "\nCurrent: {:s}"
      "\nTerminal 0 voltage: {:s}"
      "\nTerminal 1 voltage: {:s}"
      "\nVirtual Node 1 voltage: {:s}"
      "\n--- Initialization from powerflow finished ---",
      Logger::phasorToString((**mIntfVoltage)(0, 0)),
      Logger::phasorToString((**mIntfCurrent)(0, 0)),
      Logger::phasorToString(initialSingleVoltage(0)),
      Logger::phasorToString(initialSingleVoltage(1)),
      Logger::phasorToString(mVirtualNodes[0]->initialSingleVoltage()));
}

void DP::Ph1::PiLine::mnaParentAddPreStepDependencies(
    AttributeBase::List &prevStepDependencies,
    AttributeBase::List &attributeDependencies,
    AttributeBase::List &modifiedAttributes) {
  // add pre-step dependencies of component itself
  prevStepDependencies.push_back(mIntfCurrent);
  prevStepDependencies.push_back(mIntfVoltage);
  modifiedAttributes.push_back(mRightVector);
}

void DP::Ph1::PiLine::mnaParentPreStep(Real time, Int timeStepCount) {
  // pre-step of component itself
  this->mnaApplyRightSideVectorStamp(**mRightVector);
}

void DP::Ph1::PiLine::mnaParentAddPostStepDependencies(
    AttributeBase::List &prevStepDependencies,
    AttributeBase::List &attributeDependencies,
    AttributeBase::List &modifiedAttributes,
    Attribute<Matrix>::Ptr &leftVector) {
  // add post-step dependencies of component itself
  attributeDependencies.push_back(leftVector);
  modifiedAttributes.push_back(mIntfVoltage);
  modifiedAttributes.push_back(mIntfCurrent);
}

void DP::Ph1::PiLine::mnaParentPostStep(Real time, Int timeStepCount,
                                        Attribute<Matrix>::Ptr &leftVector) {
  // post-step of component itself
  this->mnaUpdateVoltage(**leftVector);
  this->mnaUpdateCurrent(**leftVector);
}

void DP::Ph1::PiLine::mnaCompUpdateVoltage(const Matrix &leftVector) {
  (**mIntfVoltage)(0, 0) = 0;
  if (terminalNotGrounded(1))
    (**mIntfVoltage)(0, 0) =
        Math::complexFromVectorElement(leftVector, matrixNodeIndex(1));
  if (terminalNotGrounded(0))
    (**mIntfVoltage)(0, 0) =
        (**mIntfVoltage)(0, 0) -
        Math::complexFromVectorElement(leftVector, matrixNodeIndex(0));
}

void DP::Ph1::PiLine::mnaCompUpdateCurrent(const Matrix &leftVector) {
  // Read the current from whichever series subcomponent is active:
  // - Inductor (standard passive line, L>=0; negative R allowed)
  // - Capacitor (series compensation, L<0)
  if (mSubSeriesInductor)
    (**mIntfCurrent)(0, 0) = mSubSeriesInductor->intfCurrent()(0, 0);
  else if (mSubSeriesCapacitor)
    (**mIntfCurrent)(0, 0) = mSubSeriesCapacitor->intfCurrent()(0, 0);
}

// #### Tear Methods ####
MNAInterface::List DP::Ph1::PiLine::mnaTearGroundComponents() {
  MNAInterface::List gndComponents;

  gndComponents.push_back(mSubParallelResistor0);
  gndComponents.push_back(mSubParallelResistor1);

  if (**mParallelCap >= 0) {
    gndComponents.push_back(mSubParallelCapacitor0);
    gndComponents.push_back(mSubParallelCapacitor1);
  }

  return gndComponents;
}

void DP::Ph1::PiLine::mnaTearInitialize(Real omega, Real timeStep) {
  mSubSeriesResistor->mnaTearSetIdx(mTearIdx);
  mSubSeriesResistor->mnaTearInitialize(omega, timeStep);
  if (mSubSeriesInductor) {
    mSubSeriesInductor->mnaTearSetIdx(mTearIdx);
    mSubSeriesInductor->mnaTearInitialize(omega, timeStep);
  }
  // DP::Ph1::Capacitor does not implement MNATearInterface; series-cap
  // lines therefore opt out of the tearing optimisation. The simulation
  // still runs correctly via the normal MNA path.
}

void DP::Ph1::PiLine::mnaTearApplyMatrixStamp(SparseMatrixRow &tearMatrix) {
  mSubSeriesResistor->mnaTearApplyMatrixStamp(tearMatrix);
  if (mSubSeriesInductor)
    mSubSeriesInductor->mnaTearApplyMatrixStamp(tearMatrix);
}

void DP::Ph1::PiLine::mnaTearApplyVoltageStamp(Matrix &voltageVector) {
  if (mSubSeriesInductor)
    mSubSeriesInductor->mnaTearApplyVoltageStamp(voltageVector);
}

void DP::Ph1::PiLine::mnaTearPostStep(Complex voltage, Complex current) {
  if (mSubSeriesInductor) {
    mSubSeriesInductor->mnaTearPostStep(voltage - current * **mSeriesRes,
                                        current);
    (**mIntfCurrent)(0, 0) = mSubSeriesInductor->intfCurrent()(0, 0);
  } else if (mSubSeriesCapacitor) {
    // No tearing-based post-step available; fall back to reading capacitor
    // current directly.
    (**mIntfCurrent)(0, 0) = mSubSeriesCapacitor->intfCurrent()(0, 0);
  }
}
